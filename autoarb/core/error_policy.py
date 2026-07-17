"""Shared classification and retry delays for LZT workflow errors."""

from __future__ import annotations

import json
import re
from typing import Any


MAINTENANCE_DELAY_SECONDS = 60 * 60
ACCOUNT_CHECK_OVERLOAD_DELAY_SECONDS = 20 * 60
TRANSIENT_DELAY_SECONDS = 10 * 60


def extract_http_status(value: Any) -> int | None:
    match = re.search(r"\bHTTP\s*([1-5]\d{2})\b", str(value or ""), re.IGNORECASE)
    return int(match.group(1)) if match else None


def is_maintenance_error(value: Any, http_status: int | None = None) -> bool:
    text = str(value or "").casefold()
    status = http_status or extract_http_status(text)
    if status == 503:
        return True
    return any(marker in text for marker in (
        "технические работы",
        "техработ",
        "маркет временно недоступен",
        "market temporarily unavailable",
        "service unavailable",
        "maintenance",
    ))


def is_invalid_credentials_error(value: Any) -> bool:
    text = str(value or "").casefold()
    return any(marker in text for marker in (
        "неверный логин или пароль у данного аккаунта",
        "неверный логин или пароль",
        "invalid login or password",
        "incorrect login or password",
    ))


def is_account_check_overload(value: Any) -> bool:
    text = str(value or "").casefold()
    return (
        "произошло более 20 ошибок во время проверки аккаунта" in text
        or ("более 20 ошибок" in text and "проверк" in text)
    )


def is_deferred_retry(value: Any) -> bool:
    """LZT accepted the check but asks the client to poll it again later."""
    return "retry_request" in str(value or "").casefold()


def deferred_retry_delay_seconds(value: Any, default: int = 60) -> int:
    """Delay a server-side pending job until its current API window resets."""
    payload = value
    if not isinstance(payload, dict):
        text = str(value or "")
        start = text.find("{")
        if start >= 0:
            try:
                payload = json.loads(text[start:])
            except (json.JSONDecodeError, TypeError, ValueError):
                payload = None
        else:
            payload = None
    rate_info: dict[str, Any] = {}
    server_time = 0
    if isinstance(payload, dict):
        system_info = payload.get("system_info")
        if isinstance(system_info, dict):
            server_time = system_info.get("time") or 0
            if isinstance(system_info.get("rate_limit"), dict):
                rate_info = system_info["rate_limit"]
    try:
        delta = int(rate_info.get("reset") or 0) - int(server_time or 0)
    except (TypeError, ValueError):
        delta = 0
    if delta <= 0:
        delta = int(default)
    # A small safety margin avoids hitting the exact reset boundary.
    return max(15, min(120, delta + 3))


def classify_retry(value: Any, http_status: int | None = None) -> dict[str, Any]:
    """Return a stable kind, user-facing label and retry decision."""
    status = http_status or extract_http_status(value)
    if is_invalid_credentials_error(value):
        return {
            "kind": "invalid_credentials",
            "label": "Неверный логин или пароль",
            "delay_seconds": 0,
            "stop_immediately": True,
            "http_status": status,
        }
    if is_maintenance_error(value, status):
        return {
            "kind": "maintenance",
            "label": "Технические работы LZT Market",
            "delay_seconds": MAINTENANCE_DELAY_SECONDS,
            "stop_immediately": False,
            "http_status": status,
        }
    if is_account_check_overload(value):
        return {
            "kind": "account_check_overload",
            "label": "Временный лимит проверки аккаунта",
            "delay_seconds": ACCOUNT_CHECK_OVERLOAD_DELAY_SECONDS,
            "stop_immediately": False,
            "http_status": status,
        }
    if is_deferred_retry(value):
        return {
            "kind": "deferred_retry",
            "label": "LZT попросил повторить запрос",
            "message": "LZT ещё обрабатывает проверку аккаунта. Запрос принят и будет повторён автоматически.",
            "delay_seconds": deferred_retry_delay_seconds(value),
            "stop_immediately": False,
            "http_status": status,
        }
    return {
        "kind": "temporary_error",
        "label": "Временная ошибка",
        "delay_seconds": TRANSIENT_DELAY_SECONDS,
        "stop_immediately": False,
        "http_status": status,
    }
