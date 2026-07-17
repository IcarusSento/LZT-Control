from __future__ import annotations

import json
import re
import threading
from datetime import datetime
from pathlib import Path

# Блокировки для thread-safe доступа к общим файлам.
# Используются и основным циклом, и фоновым sync-потоком, и веб-сервером.
dates_file_lock: threading.RLock = threading.RLock()
proliv_file_lock: threading.RLock = threading.RLock()
reference_file_lock: threading.RLock = threading.RLock()
history_file_lock: threading.RLock = threading.RLock()

DATE_FMT = "%d-%m-%Y %H:%M:%S"


def load_config(path: Path) -> dict:
    """
    Читает config.json / config.json5 как JSON5: // комментарии, хвостовые запятые.
    Обычный JSON тоже подходит. Нужен пакет: pip install json5
    """
    try:
        import json5
    except ImportError as e:
        raise ImportError(
            "Для конфига нужен пакет json5: pip install json5"
        ) from e
    if not path.is_file():
        raise FileNotFoundError(
            f"Нет файла конфига: {path}\n"
            f"Создай config.json5 (или config.json) в папке софта."
        )
    text = path.read_text(encoding="utf-8")
    raw = json5.loads(text)
    if not isinstance(raw, dict):
        raise ValueError("config: корень должен быть объектом")
    return raw


def load_dates(path: Path) -> dict[str, datetime]:
    if not path.exists():
        return {}
    dates: dict[str, datetime] = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "Дата проверки для" not in line:
                continue
            parts = line.split(": ", 1)
            if len(parts) != 2:
                continue
            item_id = parts[0].split()[-1]
            dates[item_id] = datetime.strptime(parts[1].strip(), DATE_FMT)
    return dates


def write_dates(path: Path, dates: dict[str, datetime]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for item_id in sorted(dates.keys(), key=lambda k: dates[k]):
            dt = dates[item_id]
            f.write(f"Дата проверки для {item_id}: {dt.strftime(DATE_FMT)}\n")


def sort_dates_file(path: Path) -> None:
    with dates_file_lock:
        dates = load_dates(path)
        if dates:
            write_dates(path, dates)


def load_checked_items(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open("r", encoding="utf-8") as f:
        return {ln.strip() for ln in f if ln.strip()}


def save_checked_item(path: Path, item_id: str) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(f"{item_id}\n")


def append_valid_history(path: Path, item_id: str, outcome: str) -> None:
    stamp = datetime.now().strftime(DATE_FMT)
    with history_file_lock:
        with path.open("a", encoding="utf-8") as f:
            f.write(f"{stamp} | item {item_id} | {outcome}\n")


def append_pipeline_line(path: Path, item_id: str, line: str) -> None:
    stamp = datetime.now().strftime(DATE_FMT)
    with history_file_lock:
        with path.open("a", encoding="utf-8") as f:
            f.write(f"{stamp} | item {item_id} | {line}\n")


def load_proliv_queue(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, list) else []
    except (json.JSONDecodeError, OSError):
        return []


def save_proliv_queue(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)


def append_proliv_history(path: Path, item_id: str, outcome: str) -> None:
    stamp = datetime.now().strftime(DATE_FMT)
    with history_file_lock:
        with path.open("a", encoding="utf-8") as f:
            f.write(f"{stamp} | item {item_id} | {outcome}\n")


def remove_reference_item(path: Path, item_id: str) -> bool:
    """Atomically remove an account from a line-based reference file."""
    item_id = str(item_id)
    pattern = re.compile(rf"(?<!\d){re.escape(item_id)}(?!\d)")
    with reference_file_lock:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines() if path.exists() else []
        kept = [line for line in lines if not pattern.search(line)]
        if len(kept) == len(lines):
            return False
        temp = path.with_suffix(path.suffix + ".tmp")
        temp.write_text(("\n".join(kept) + "\n") if kept else "", encoding="utf-8")
        temp.replace(path)
        return True


resold_file_lock: threading.RLock = threading.RLock()
valid_errors_file_lock: threading.RLock = threading.RLock()


def load_resold_items(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, list) else []
    except (json.JSONDecodeError, OSError):
        return []


def save_resold_item(path: Path, item_id: str, new_item_id: str, detected_at: str) -> None:
    """Insert or refresh one confirmed resale without creating duplicates."""
    with resold_file_lock:
        items = load_resold_items(path)
        existing = next(
            (row for row in items if str(row.get("item_id")) == str(item_id)),
            None,
        )
        clean_new_id = str(new_item_id or "").strip()
        if existing is None:
            items.append({
                "item_id": str(item_id),
                "new_item_id": clean_new_id,
                "detected_at": detected_at,
            })
        else:
            # fast-sell can confirm publication before Same IDs exposes the new
            # listing.  Keep the row visible immediately and fill the ID later.
            if clean_new_id and clean_new_id.casefold() not in {"unknown", "none", "?"}:
                existing["new_item_id"] = clean_new_id
            if not existing.get("detected_at"):
                existing["detected_at"] = detected_at
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(items, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Validation errors (retry / maintenance)
# ---------------------------------------------------------------------------

def load_validation_errors(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, list) else []
    except (json.JSONDecodeError, OSError):
        return []


def save_validation_errors(path: Path, errors: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(errors, f, indent=2, ensure_ascii=False)


def upsert_validation_error(path: Path, lock: threading.RLock, err_key: str, entry: dict) -> None:
    """Thread-safe upsert записи об ошибке валидации."""
    with lock:
        errors = load_validation_errors(path)
        idx = next((i for i, e in enumerate(errors) if e.get("err_key") == err_key), -1)
        if idx >= 0:
            errors[idx] = entry
        else:
            errors.append(entry)
        save_validation_errors(path, errors)


def dismiss_validation_error(path: Path, lock: threading.RLock, err_key: str) -> bool:
    """Удаляет запись об ошибке (dismiss или после успешного recheck)."""
    with lock:
        errors = load_validation_errors(path)
        new_errors = [e for e in errors if e.get("err_key") != err_key]
        changed = len(new_errors) < len(errors)
        if changed:
            save_validation_errors(path, new_errors)
        return changed


# ---------------------------------------------------------------------------
# Transfer log + settings + transferred items
# ---------------------------------------------------------------------------

transfer_items_lock: threading.RLock = threading.RLock()


def append_transfer_log(path: Path, item_id: str, recipient: str, result: str) -> None:
    stamp = datetime.now().strftime(DATE_FMT)
    with path.open("a", encoding="utf-8") as f:
        f.write(f"{stamp} | item {item_id} | → {recipient} | {result}\n")


def load_transfer_settings(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def save_transfer_settings(path: Path, settings: dict) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(settings, f, indent=2, ensure_ascii=False)


def load_transferred_items(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return set(str(x) for x in data) if isinstance(data, list) else set()
    except (json.JSONDecodeError, OSError):
        return set()


def add_transferred_item(path: Path, lock: threading.RLock, item_id: str) -> None:
    with lock:
        items = load_transferred_items(path)
        items.add(str(item_id))
        with path.open("w", encoding="utf-8") as f:
            json.dump(sorted(items), f, indent=2, ensure_ascii=False)
