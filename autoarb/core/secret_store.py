from __future__ import annotations

import base64
import ctypes
import os
import threading
from ctypes import wintypes
from pathlib import Path


_lock = threading.RLock()
_CRYPTPROTECT_UI_FORBIDDEN = 0x01
_TEXT_PREFIX = "dpapi:"


class _DataBlob(ctypes.Structure):
    _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_byte))]


def _blob(data: bytes) -> tuple[_DataBlob, object]:
    buffer = ctypes.create_string_buffer(data)
    value = _DataBlob(len(data), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_byte)))
    return value, buffer


def _protect(data: bytes) -> bytes:
    if os.name != "nt":
        raise RuntimeError("Защищённое хранилище секретов поддерживается только Windows")
    source, source_buffer = _blob(data)
    result = _DataBlob()
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    ok = crypt32.CryptProtectData(
        ctypes.byref(source),
        ctypes.c_wchar_p("LZT Control local secret"),
        None,
        None,
        None,
        _CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(result),
    )
    del source_buffer
    if not ok:
        raise ctypes.WinError()
    try:
        return ctypes.string_at(result.pbData, result.cbData)
    finally:
        kernel32.LocalFree(result.pbData)


def _unprotect(data: bytes) -> bytes:
    if os.name != "nt":
        raise RuntimeError("Защищённое хранилище секретов поддерживается только Windows")
    source, source_buffer = _blob(data)
    result = _DataBlob()
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    ok = crypt32.CryptUnprotectData(
        ctypes.byref(source),
        None,
        None,
        None,
        None,
        _CRYPTPROTECT_UI_FORBIDDEN,
        ctypes.byref(result),
    )
    del source_buffer
    if not ok:
        raise ctypes.WinError()
    try:
        return ctypes.string_at(result.pbData, result.cbData)
    finally:
        kernel32.LocalFree(result.pbData)


def save_secret(path: Path, value: str) -> None:
    value = str(value or "")
    with _lock:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not value:
            path.unlink(missing_ok=True)
            return
        encoded = base64.b64encode(_protect(value.encode("utf-8")))
        temp = path.with_suffix(path.suffix + ".tmp")
        temp.write_bytes(encoded)
        temp.replace(path)


def protect_text(value: str) -> str:
    """Encrypt a short value for the current Windows user (DPAPI)."""
    plain = str(value or "")
    if not plain or plain.startswith(_TEXT_PREFIX):
        return plain
    encrypted = base64.b64encode(_protect(plain.encode("utf-8"))).decode("ascii")
    return _TEXT_PREFIX + encrypted


def unprotect_text(value: str) -> str:
    """Decrypt DPAPI text; return legacy plaintext unchanged for migration."""
    stored = str(value or "")
    if not stored.startswith(_TEXT_PREFIX):
        return stored
    try:
        encrypted = base64.b64decode(stored[len(_TEXT_PREFIX):].encode("ascii"), validate=True)
        return _unprotect(encrypted).decode("utf-8")
    except Exception as exc:
        raise RuntimeError(
            "Секрет зашифрован для другого пользователя Windows; введи его заново"
        ) from exc


def load_secret(path: Path) -> str:
    with _lock:
        if not path.exists():
            return ""
        try:
            encrypted = base64.b64decode(path.read_bytes(), validate=True)
            return _unprotect(encrypted).decode("utf-8")
        except Exception as exc:
            raise RuntimeError("Не удалось расшифровать локальный секрет для текущего пользователя Windows") from exc


def has_secret(path: Path) -> bool:
    return path.is_file() and path.stat().st_size > 0
