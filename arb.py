import json
import logging
import re
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from autoarb.core.proliv_options import DEFAULT_EXTRA_GAMES, normalize_extra_games
from services.arb.stats import (
    collect_invalid_accounts,
    collect_proliv_queue_errors,
    collect_tracked_guarantee_records,
    compute_dashboard_stats,
)

ARB_DIR = Path(__file__).parent / "autoarb"

logger = logging.getLogger("lzt_arb")

ARB_AVAILABLE = True
ARB_IMPORT_ERROR = ""
try:
    from autoarb.core.orders_sync import run_sync_cycle, slot_label
    from autoarb.core.error_policy import deferred_retry_delay_seconds, is_maintenance_error
    from autoarb.core.paths import (
        BLACKLIST_TXT,
        CHECKED_ITEMS,
        CLAIM_HISTORY,
        CONFIG_SECRETS,
        CONFIG_PATH,
        DATES_OF_CHECK,
        GUARANTEE_TXT,
        PIPELINE_LOG,
        PROLIV_HISTORY,
        PROLIV_QUEUE,
        RESOLD_FILE,
        TELEGRAM_ERR_LOG,
        TRANSFER_LOG,
        TRANSFER_SECRET,
        TRANSFER_SETTINGS,
        TRANSFERRED_ITEMS,
        VALID_HISTORY,
        VALIDATION_ERRORS,
    )
    from autoarb.core.proliv import normalize_deferred_proliv_rows, next_proliv_scheduled, run_proliv_due
    from autoarb.core.storage import (
        DATE_FMT,
        add_transferred_item,
        append_proliv_history,
        append_transfer_log,
        dates_file_lock,
        dismiss_validation_error,
        load_checked_items,
        load_dates,
        load_proliv_queue,
        load_resold_items,
        load_transfer_settings,
        load_transferred_items,
        load_validation_errors,
        history_file_lock,
        proliv_file_lock,
        reference_file_lock,
        resold_file_lock,
        save_proliv_queue,
        save_validation_errors,
        save_transfer_settings,
        transfer_items_lock,
        upsert_validation_error,
        valid_errors_file_lock,
        write_dates,
    )
    from autoarb.core.notify import send_telegram
    from autoarb.core.secret_store import has_secret, load_secret, save_secret
    from autoarb.core.throttled_client import ThrottledClient, use_lzt_proxy_from_config
    from autoarb.core.transfer import get_pending_transfers, transfer_item
    from autoarb.core.valid_check import next_scheduled, run_valid_due
except Exception as exc:
    ARB_AVAILABLE = False
    ARB_IMPORT_ERROR = str(exc)
    logger.warning("AutoARB недоступен: %s", exc)


# ── Config ───────────────────────────────────────────────────────────────────
ARB_DEFAULTS: Dict[str, Any] = {
    "enabled": False,
    "nickname": "",
    "user_id": "",
    "token": "",
    "lzt_api_base": "https://api.lzt.market",
    "link": "",
    "orders_list_use_path_user_id": False,
    "use_proxy": False,
    "proxy": "",
    "telegram_enabled": False,
    "telegram_token": "",
    "telegram_chat_id": "",
    "telegram_friend_enabled": False,
    "telegram_friend_token": "",
    "telegram_friend_chat_id": "",
    "telegram_bots": [],
    "telegram_separate_errors": False,
    "telegram_error_bot_index": -1,
    "telegram_notification_template": "{icon} {title}\n\n{message}\n\n🕒 {time} · LZT Control",
    "telegram_error_template": "🚨 {title}\n\n{message}\n\n🕒 {time} · Требуется внимание",
    "steam_web_api_key": "",
    "check_schedule_percents": [10, 55, 99],
    "orders_stop_without_guarantee_enabled": True,
    "orders_no_guarantee_page_limit": 2,
    "kt_enabled": True,
    "kt_require_steam_link": True,
    "auto_claim_enabled": True,
    "claim_min_interval_seconds": 60,
    "proliv_enabled": True,
    "proliv_after_warranty_seconds": 60,
    "proliv_prefetch_goods_add": True,
    "proliv_list_title": "666",
    "proliv_list_price_rub": 1000000,
    "proliv_min_price_rub": 10,
    "proliv_default_category_id": 1,
    "proliv_currency": "rub",
    "proliv_extended_guarantee": 0,
    "proliv_guarantee_duration": 86400,
    "proliv_fast_sell_max_attempts": 100,
    "proliv_resell_id_in_item_add": False,
    "proliv_goods_check_use_query": True,
    "proliv_goods_check_close_item": True,
    "proliv_after_publish_tag_enabled": True,
    "proliv_after_publish_tag_id": 23,
    "proliv_retry_max": 5,
    "proliv_extra_games": dict(DEFAULT_EXTRA_GAMES),
    "sync_interval_seconds": 1800,
    "request_delay_seconds": 3,
    "valid_retry_max": 5,
    "transfer_recipients": [],
}

_config_lock = threading.RLock()
_CONFIG_SECRET_FIELDS = {
    "nickname", "user_id", "token", "link", "proxy",
    "telegram_token", "telegram_chat_id", "telegram_friend_token",
    "telegram_friend_chat_id", "telegram_bots", "steam_web_api_key",
    "transfer_recipients",
}


def _config_file() -> Path:
    return CONFIG_PATH if ARB_AVAILABLE else ARB_DIR / "config" / "config.json5"


def load_arb_config() -> Dict[str, Any]:
    cfg = dict(ARB_DEFAULTS)
    path = _config_file()
    if path.is_file():
        text = path.read_text(encoding="utf-8")
        raw: Optional[Dict] = None
        try:
            raw = json.loads(text)
        except json.JSONDecodeError:
            try:
                import json5
                raw = json5.loads(text)
            except Exception:
                raw = None
        if isinstance(raw, dict):
            cfg.update(raw)
            if "telegram_bots" not in raw:
                migrated_bots = []
                main_token = str(raw.get("telegram_token") or "").strip()
                main_chat = str(raw.get("telegram_chat_id") or "").strip()
                if raw.get("telegram_enabled") and main_token and main_chat:
                    migrated_bots.append({
                        "name": "Основной бот", "token": main_token,
                        "chat_id": main_chat, "enabled": True,
                    })
                friend_chat = str(raw.get("telegram_friend_chat_id") or "").strip()
                friend_token = str(raw.get("telegram_friend_token") or main_token).strip()
                if raw.get("telegram_friend_enabled") and friend_token and friend_chat:
                    migrated_bots.append({
                        "name": "Второй получатель", "token": friend_token,
                        "chat_id": friend_chat, "enabled": True,
                    })
                cfg["telegram_bots"] = migrated_bots[:5]
            migrated_secrets = {
                key: cfg.get(key) for key in _CONFIG_SECRET_FIELDS if key in raw
            }
            if ARB_AVAILABLE and migrated_secrets:
                existing_secrets: Dict[str, Any] = {}
                if has_secret(CONFIG_SECRETS):
                    try:
                        decoded = json.loads(load_secret(CONFIG_SECRETS))
                        if isinstance(decoded, dict):
                            existing_secrets.update(decoded)
                    except Exception:
                        pass
                existing_secrets.update(migrated_secrets)
                save_secret(CONFIG_SECRETS, json.dumps(existing_secrets, ensure_ascii=False))
                clean = {key: value for key, value in raw.items() if key not in _CONFIG_SECRET_FIELDS}
                temp = path.with_suffix(path.suffix + ".tmp")
                temp.write_text(json.dumps(clean, indent=2, ensure_ascii=False), encoding="utf-8")
                temp.replace(path)
            legacy_secret = str(cfg.pop("transfer_secret_answer", "") or "").strip()
            if ARB_AVAILABLE and legacy_secret and not has_secret(TRANSFER_SECRET):
                save_secret(TRANSFER_SECRET, legacy_secret)
            if ARB_AVAILABLE and "transfer_secret_answer" in raw:
                clean = {key: value for key, value in raw.items() if key not in _CONFIG_SECRET_FIELDS}
                clean.pop("transfer_secret_answer", None)
                temp = path.with_suffix(path.suffix + ".tmp")
                temp.write_text(json.dumps(clean, indent=2, ensure_ascii=False), encoding="utf-8")
                temp.replace(path)
    try:
        if not ARB_AVAILABLE:
            raise RuntimeError("Модули проверки недоступны")
        if has_secret(CONFIG_SECRETS):
            protected_cfg = json.loads(load_secret(CONFIG_SECRETS))
            if isinstance(protected_cfg, dict):
                cfg.update({key: value for key, value in protected_cfg.items() if key in _CONFIG_SECRET_FIELDS})
        cfg["transfer_secret_answer"] = load_secret(TRANSFER_SECRET)
        cfg["transfer_secret_answer_set"] = bool(cfg["transfer_secret_answer"])
    except Exception as exc:
        logger.error("[ARB] Не удалось открыть защищённый секрет передачи: %s", exc)
        cfg["transfer_secret_answer"] = ""
        cfg["transfer_secret_answer_set"] = False
    return cfg


def save_arb_config(cfg: Dict[str, Any]) -> None:
    path = _config_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    safe_cfg = dict(cfg)
    safe_cfg.pop("transfer_secret_answer", None)
    safe_cfg.pop("transfer_secret_answer_set", None)
    protected_cfg: Dict[str, Any] = {}
    if ARB_AVAILABLE:
        protected_cfg = {
            key: safe_cfg.pop(key) for key in list(safe_cfg) if key in _CONFIG_SECRET_FIELDS
        }
    with _config_lock:
        if ARB_AVAILABLE:
            save_secret(CONFIG_SECRETS, json.dumps(protected_cfg, ensure_ascii=False))
        temp = path.with_suffix(path.suffix + ".tmp")
        temp.write_text(json.dumps(safe_cfg, indent=2, ensure_ascii=False), encoding="utf-8")
        temp.replace(path)


# ── Helpers ──────────────────────────────────────────────────────────────────
def _parse_item_id(raw: str) -> Optional[str]:
    raw = raw.strip()
    m = re.search(r"lzt(?:\.market|forum\.com)/(\d+)", raw)
    if m:
        return m.group(1)
    if raw.isdigit():
        return raw
    return None


def _parse_item_ids_bulk(text: str) -> List[str]:
    ids: List[str] = []
    seen: set = set()
    for m in re.finditer(r"lzt(?:\.market|forum\.com)/(\d+)", text):
        iid = m.group(1)
        if iid not in seen:
            seen.add(iid)
            ids.append(iid)
    remaining = re.sub(r"lzt(?:\.market|forum\.com)/\d+/?", "", text)
    for m in re.finditer(r"\b(\d{6,12})\b", remaining):
        iid = m.group(1)
        if iid not in seen:
            seen.add(iid)
            ids.append(iid)
    return ids


def _tail(path: Path, n: int = 100) -> List[str]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8", errors="replace") as handle:
        lines = deque((line.rstrip("\r\n") for line in handle if line.strip()), maxlen=n)
    return list(lines)


def _all_lines(path: Path) -> List[str]:
    if not path.exists():
        return []
    return [ln.strip() for ln in path.read_text(encoding="utf-8", errors="replace").splitlines() if ln.strip()]


def _reference_records(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with reference_file_lock:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    records: List[Dict[str, Any]] = []
    for index, value in enumerate(lines):
        value = value.strip()
        if not value:
            continue
        match = re.search(r"(?:Item\s*ID\s*:\s*|\b)(\d{6,12})\b", value, re.I)
        records.append({
            "line_index": index,
            "value": value,
            "item_id": match.group(1) if match else "",
        })
    return records


def _fmt_unix(unix: int) -> str:
    try:
        return datetime.fromtimestamp(int(unix)).strftime("%d-%m-%Y %H:%M:%S")
    except Exception:
        return str(unix)


def _count_lines(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(encoding="utf-8", errors="replace") as f:
        return sum(1 for ln in f if ln.strip())


def _is_internal_pending_error(row: Dict[str, Any]) -> bool:
    if str(row.get("error_kind") or row.get("last_error_type") or "").casefold() == "deferred_retry":
        return True
    try:
        blob = json.dumps(row, ensure_ascii=False).casefold()
    except (TypeError, ValueError):
        blob = str(row).casefold()
    return "retry_request" in blob


def _is_invalid_account_result(row: Dict[str, Any]) -> bool:
    """Expected account result, not an operational application error."""
    kind = str(row.get("error_kind") or row.get("last_error_type") or "").casefold()
    if kind == "invalid_credentials":
        return True
    text = " ".join(
        str(row.get(key) or "")
        for key in ("error_label", "error_message", "last_error_type")
    ).casefold()
    return "неверный логин или пароль" in text or "invalid login or password" in text


def _load_actionable_validation_errors() -> List[Dict[str, Any]]:
    """Drop legacy retry_request rows; they are scheduler state, not errors."""
    with valid_errors_file_lock:
        errors = load_validation_errors(VALIDATION_ERRORS)
        actionable = [row for row in errors if not _is_internal_pending_error(row)]
        pending = [row for row in errors if _is_internal_pending_error(row)]
        if len(actionable) != len(errors):
            save_validation_errors(VALIDATION_ERRORS, actionable)
    # Older versions removed the current check after misclassifying HTTP 200.
    # Restore that exact slot after releasing the error lock to keep lock order
    # safe relative to the background worker.
    if pending:
        with dates_file_lock:
            dates = load_dates(DATES_OF_CHECK)
            changed = False
            now = datetime.now()
            for row in pending:
                item_id = str(row.get("item_id") or "")
                key = str(row.get("err_key") or "")
                if not item_id or not (key == item_id or key.startswith(f"{item_id}#")):
                    continue
                delay = deferred_retry_delay_seconds(row.get("api_response") or row)
                dates[key] = now + timedelta(seconds=delay)
                changed = True
            if changed:
                write_dates(DATES_OF_CHECK, dates)
    return actionable


def _collect_invalid_account_rows(
    validation_errors: List[Dict[str, Any]],
    proliv: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows = collect_invalid_accounts(validation_errors, PIPELINE_LOG, CLAIM_HISTORY)
    known = {str(row.get("item_id") or "") for row in rows}
    for error in collect_proliv_queue_errors(proliv, PROLIV_HISTORY):
        if not _is_invalid_account_result(error):
            continue
        item_id = str(error.get("item_id") or "")
        if not item_id or item_id in known:
            continue
        rows.append({
            "item_id": item_id,
            "kind": "invalid",
            "kind_label": "Невалид",
            "reason": str(error.get("error_label") or "Неверный логин или пароль"),
            "slot_label": str(error.get("slot_label") or "Пролив"),
            "detected_at": str(error.get("last_error_at") or "—"),
            "claim_status": "",
            "claim_message": "",
            "api_response": str(error.get("api_response") or ""),
        })
        known.add(item_id)
    return rows


def _error_sort_timestamp(row: Dict[str, Any]) -> float:
    """Return one comparable timestamp for errors from every subsystem."""
    try:
        unix_value = float(row.get("last_error_at_unix") or 0)
        if unix_value > 0:
            return unix_value
    except (TypeError, ValueError):
        pass

    raw = str(row.get("last_error_at") or row.get("first_error_at") or "").strip()
    for pattern in (
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d.%m.%Y %H:%M:%S",
    ):
        try:
            return datetime.strptime(raw, pattern).timestamp()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return 0.0


# ── Service ──────────────────────────────────────────────────────────────────
class ArbService:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._recovery_lock = threading.RLock()
        self._recovery_pending_keys: set[str] = set()
        self._recovery_acknowledged_keys: set[str] = set()
        self._stop = threading.Event()
        self._sync_now = threading.Event()
        self._threads: List[threading.Thread] = []
        self.client: Optional["ThrottledClient"] = None
        self.config: Dict[str, Any] = {}
        self.running = False
        self.state: Dict[str, Any] = {
            "sync_eta": 0, "sync_note": "", "sync_busy": False,
            "sync_log": [], "last_error": "", "started_at": "",
            "pipeline_busy": False, "pipeline_kind": "", "pipeline_item": "",
            "pipeline_key": "", "pipeline_note": "", "pipeline_retry_eta": 0,
            "activity_log": [],
            "recovery_required": False, "recovery_checks": 0,
            "recovery_accounts": 0,
        }

    def _capture_recovery(self) -> None:
        if not ARB_AVAILABLE:
            return
        now = datetime.now()
        with dates_file_lock:
            dates = load_dates(DATES_OF_CHECK)
        due = {key for key, when in dates.items() if when <= now}
        with self._recovery_lock:
            self._recovery_acknowledged_keys.intersection_update(due)
            pending = due - self._recovery_acknowledged_keys
            self._recovery_pending_keys = pending
            accounts = {key.split("#", 1)[0] for key in pending}
            self.state.update({
                "recovery_required": bool(pending),
                "recovery_checks": len(pending),
                "recovery_accounts": len(accounts),
            })

    def resolve_recovery(self, removed_keys: set[str], scheduled_keys: set[str]) -> None:
        with self._recovery_lock:
            self._recovery_pending_keys.difference_update(removed_keys)
            self._recovery_pending_keys.difference_update(scheduled_keys)
            self._recovery_acknowledged_keys.difference_update(removed_keys)
            self._recovery_acknowledged_keys.update(scheduled_keys)
            accounts = {key.split("#", 1)[0] for key in self._recovery_pending_keys}
            self.state.update({
                "recovery_required": bool(self._recovery_pending_keys),
                "recovery_checks": len(self._recovery_pending_keys),
                "recovery_accounts": len(accounts),
            })

    def _activity(self, message: str, level: str = "info") -> None:
        line = {
            "time": datetime.now().strftime("%H:%M:%S"),
            "level": level,
            "message": str(message).replace("\n", " ")[:500],
        }
        log = list(self.state.get("activity_log") or [])
        log.append(line)
        self.state["activity_log"] = log[-80:]

    def _set_error(self, exc: Exception, where: str) -> None:
        self.state["last_error"] = f"{datetime.now().strftime('%H:%M:%S')} [{where}] {exc}"
        logger.exception("[ARB] Ошибка (%s): %s", where, exc)

    def start(self) -> None:
        with self._lock:
            if self.running:
                return
            if not ARB_AVAILABLE:
                raise HTTPException(503, f"Модули проверки недоступны: {ARB_IMPORT_ERROR}")
            cfg = load_arb_config()
            token = str(cfg.get("token") or "").strip()
            if not token:
                raise HTTPException(400, "В настройках проверки не задан API токен")
            self.config = cfg
            use_px, proxy_line = use_lzt_proxy_from_config(cfg)
            self.client = ThrottledClient(
                token=token,
                proxy=proxy_line,
                delay_seconds=float(cfg.get("request_delay_seconds", 3)),
                use_lzt_proxy=use_px,
                source="Проверка",
            )
            self._stop = threading.Event()
            self._sync_now = threading.Event()
            self.state.update({
                "sync_eta": 0, "sync_note": "старт…", "sync_busy": True,
                "sync_log": [], "last_error": "",
                "pipeline_busy": False, "pipeline_kind": "", "pipeline_item": "",
                "pipeline_key": "", "pipeline_note": "", "pipeline_retry_eta": 0,
                "activity_log": [],
                "started_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            })
            migrated_pending = normalize_deferred_proliv_rows(PROLIV_QUEUE)
            self._capture_recovery()
            if migrated_pending:
                self._activity(
                    f"В очередь возвращено внутренних ожиданий LZT: {migrated_pending}",
                    "info",
                )
            self._activity("Сервис проверки запущен", "success")
            if self.state.get("recovery_required"):
                self._activity(
                    f"Ожидается решение по пропущенным проверкам: {self.state['recovery_checks']}",
                    "warning",
                )
            stop = self._stop
            self._threads = [
                threading.Thread(target=self._sync_loop, args=(stop,), daemon=True, name="arb-sync"),
                threading.Thread(target=self._due_loop, args=(stop,), daemon=True, name="arb-pipeline"),
                threading.Thread(target=self._transfer_loop, args=(stop,), daemon=True, name="arb-transfer"),
            ]
            for t in self._threads:
                t.start()
            self.running = True
            logger.info("[ARB] Сервис запущен (опрос каждые %s сек)", cfg.get("sync_interval_seconds", 1800))

    def stop(self) -> None:
        with self._lock:
            if not self.running:
                return
            self._stop.set()
            for t in self._threads:
                t.join(timeout=3)
            self._threads = []
            self.running = False
            self.state["sync_busy"] = False
            self.state["pipeline_busy"] = False
            self._activity("Сервис проверки остановлен", "info")
            logger.info("[ARB] Сервис остановлен")

    def restart_if_running(self) -> None:
        if self.running:
            self.stop()
            self.start()

    def get_client(self) -> "ThrottledClient":
        if self.running and self.client:
            return self.client
        cfg = load_arb_config()
        token = str(cfg.get("token") or "").strip()
        if not token:
            raise HTTPException(400, "В настройках проверки не задан API токен")
        use_px, proxy_line = use_lzt_proxy_from_config(cfg)
        return ThrottledClient(
            token=token, proxy=proxy_line,
            delay_seconds=float(cfg.get("request_delay_seconds", 3)),
            use_lzt_proxy=use_px,
            source="Проверка",
        )

    def _sync_loop(self, stop: threading.Event) -> None:
        interval = max(60, int(self.config.get("sync_interval_seconds", 1800)))
        sync_failures = 0
        while not stop.is_set():
            next_wait = interval
            self.state["sync_busy"] = True
            self.state["sync_note"] = "заказы и гарантии"
            self._activity("Запрашиваем покупки и статусы гарантий", "working")
            try:
                new_n, total = run_sync_cycle(self.client, self.config, CHECKED_ITEMS, ui_state=self.state)
                sync_failures = 0
                dismiss_validation_error(
                    VALIDATION_ERRORS, valid_errors_file_lock, "sync#orders",
                )
                self.state["sync_note"] = f"+{new_n} новых, на гарантии {total}"
                self._activity(
                    f"Синхронизация завершена: новых {new_n}, на гарантии {total}",
                    "success",
                )
                logger.info("[ARB] Опрос покупок: +%d новых, на гарантии %d", new_n, total)
            except Exception as exc:
                sync_failures += 1
                maintenance = is_maintenance_error(exc)
                next_wait = 3600 if maintenance else min(interval, min(300, 30 * (2 ** min(sync_failures - 1, 4))))
                self.state["sync_note"] = f"ошибка: {exc}"
                retry_label = "через 1 час" if maintenance else f"через {next_wait} сек"
                self._activity(f"Ошибка синхронизации: {exc}. Повтор {retry_label}", "error")
                self._set_error(exc, "опрос покупок")
                now_label = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                with valid_errors_file_lock:
                    stored_errors = load_validation_errors(VALIDATION_ERRORS)
                    previous = next((row for row in stored_errors if row.get("err_key") == "sync#orders"), None)
                upsert_validation_error(VALIDATION_ERRORS, valid_errors_file_lock, "sync#orders", {
                    "err_key": "sync#orders", "item_id": "", "slot": "sync",
                    "slot_label": "Покупки", "source": "system",
                    "stage": "Опрос списка покупок",
                    "first_error_at": previous.get("first_error_at") if previous else now_label,
                    "last_error_at": now_label, "retry_count": sync_failures,
                    "max_retries": 0, "exhausted": False,
                    "last_error_type": "maintenance" if maintenance else "sync_error",
                    "error_kind": "maintenance" if maintenance else "sync_error",
                    "error_label": "Технические работы LZT Market" if maintenance else "Ошибка синхронизации",
                    "error_message": str(exc)[:1000], "endpoint": "Список заказов",
                    "http_status": 503 if maintenance else None,
                    "api_response": str(exc)[:4000],
                    "next_retry_at": (datetime.now() + timedelta(seconds=next_wait)).strftime("%d-%m-%Y %H:%M:%S"),
                })
                try:
                    send_telegram(self.client, f"⚠️ Проверка: ошибка опроса покупок\n{exc!s}"[:3900], self.config)
                except Exception:
                    pass
            self.state["sync_busy"] = False
            self.state["sync_eta"] = next_wait
            while self.state["sync_eta"] > 0:
                if stop.wait(1):
                    return
                if self._sync_now.is_set():
                    self._sync_now.clear()
                    break
                self.state["sync_eta"] -= 1

    def _next_due_target(self) -> tuple[str, str, str]:
        """Return (kind, item_id, schedule_key) for the work being picked up."""
        now_unix = int(time.time())
        with proliv_file_lock:
            queue = load_proliv_queue(PROLIV_QUEUE)
        due_proliv = [
            r for r in queue
            if not r.get("manual_review") and int(r.get("run_at_unix", 0)) <= now_unix
        ]
        if due_proliv:
            row = min(due_proliv, key=lambda r: int(r.get("run_at_unix", 0)))
            item_id = str(row.get("item_id", ""))
            return "proliv", item_id, item_id

        with self._recovery_lock:
            if self._recovery_pending_keys:
                return "", "", ""

        with dates_file_lock:
            dates = load_dates(DATES_OF_CHECK)
        nxt = next_scheduled(dates)
        if nxt and nxt[1] <= datetime.now():
            key = nxt[0]
            return "check", key.split("#", 1)[0], key
        return "", "", ""

    def _wait_pipeline_retry(self, stop: threading.Event, seconds: int) -> None:
        self.state["pipeline_retry_eta"] = seconds
        while self.state["pipeline_retry_eta"] > 0:
            if stop.wait(1):
                return
            self.state["pipeline_retry_eta"] -= 1

    def _due_loop(self, stop: threading.Event) -> None:
        network_failures = 0
        while not stop.is_set():
            kind, item_id, key = self._next_due_target()
            self.state.update({
                "pipeline_busy": bool(kind),
                "pipeline_kind": kind,
                "pipeline_item": item_id,
                "pipeline_key": key,
                "pipeline_note": ("Отправляем запрос на пролив…" if kind == "proliv" else
                                  "Отправляем запрос на проверку…" if kind == "check" else ""),
                "pipeline_retry_eta": 0,
            })
            if kind:
                action = "Проверяем" if kind == "check" else "Выполняем пролив"
                self._activity(f"{action} аккаунт #{item_id}", "working")
            try:
                # Dispatch only the target selected by _next_due_target().  In
                # particular, an empty target means recovery is waiting for the
                # user's decision and no overdue check may be consumed here.
                if kind == "proliv":
                    def on_fast_sell_attempt(attempt: int, maximum: int, state: str) -> None:
                        if state == "retry_request":
                            next_attempt = min(maximum, attempt + 1)
                            self.state["pipeline_note"] = (
                                f"LZT вернул retry_request · готовим повтор "
                                f"{next_attempt}/{maximum}"
                            )
                        else:
                            self.state["pipeline_note"] = (
                                f"Fast-sell · попытка {attempt}/{maximum} · "
                                "ожидаем ответ LZT"
                            )

                    worked = run_proliv_due(
                        self.client, self.config, PROLIV_QUEUE, PROLIV_HISTORY,
                        VALIDATION_ERRORS,
                        on_fast_sell_attempt=on_fast_sell_attempt,
                    )
                elif kind == "check":
                    worked = run_valid_due(
                        self.client, self.config, DATES_OF_CHECK, VALID_HISTORY,
                        quiet=True,
                    )
                else:
                    worked = False
                if network_failures and "[сеть]" in str(self.state.get("last_error", "")):
                    self.state["last_error"] = ""
                network_failures = 0
                if kind and worked:
                    self._activity(f"Обработка #{item_id} завершена", "success")
                self.state.update({
                    "pipeline_busy": False, "pipeline_kind": "", "pipeline_item": "",
                    "pipeline_key": "", "pipeline_note": "", "pipeline_retry_eta": 0,
                })
                if not worked:
                    stop.wait(1.0)
            except (requests.ConnectionError, requests.Timeout) as exc:
                network_failures += 1
                delay = min(120, 10 * (2 ** min(network_failures - 1, 3)))
                short_error = str(exc).replace("\n", " ")[:240]
                self.state.update({
                    "pipeline_busy": False,
                    "pipeline_note": f"Сеть недоступна — повтор через {delay} сек",
                    "last_error": f"{datetime.now().strftime('%H:%M:%S')} [сеть] {short_error}",
                })
                logger.warning("[ARB] Сеть недоступна (%s). Повтор через %d сек", short_error, delay)
                self._activity(f"Сеть недоступна. Повтор через {delay} сек: {short_error}", "warning")
                self._wait_pipeline_retry(stop, delay)
            except Exception as exc:
                self._set_error(exc, "цикл проверок")
                self.state.update({
                    "pipeline_busy": False,
                    "pipeline_note": "Ошибка цикла — безопасный повтор через 15 сек",
                })
                self._activity(f"Ошибка цикла: {exc}. Безопасный повтор через 15 сек", "error")
                self._wait_pipeline_retry(stop, 15)

    def _transfer_loop(self, stop: threading.Event) -> None:
        while not stop.wait(60):
            try:
                s = load_transfer_settings(TRANSFER_SETTINGS)
                if not s.get("auto_enabled"):
                    continue
                interval_sec = max(60, int(s.get("interval_minutes", 60))) * 60
                last_run = float(s.get("last_auto_run_unix", 0) or 0)
                if (time.time() - last_run) < interval_sec:
                    continue
                recipient = str(s.get("recipient") or "").strip()
                if not recipient:
                    continue
                pending = get_pending_transfers(PROLIV_HISTORY, TRANSFERRED_ITEMS)
                for iid in pending:
                    ok, msg = transfer_item(self.client, self.config, iid, recipient)
                    if ok:
                        add_transferred_item(TRANSFERRED_ITEMS, transfer_items_lock, iid)
                    append_transfer_log(TRANSFER_LOG, iid, recipient,
                                        "auto: ok" if ok else f"auto: fail: {msg}")
                s["last_auto_run_unix"] = int(time.time())
                s["last_auto_run"] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                save_transfer_settings(TRANSFER_SETTINGS, s)
            except Exception as exc:
                self._set_error(exc, "авто-передача")

    def status(self) -> Dict[str, Any]:
        if not self.running:
            self._capture_recovery()
        cfg = load_arb_config()
        out: Dict[str, Any] = {
            "ok": True,
            "available": ARB_AVAILABLE,
            "running": self.running,
            "enabled": bool(cfg.get("enabled")),
            "token_set": bool(str(cfg.get("token") or "").strip()),
            "sync_eta": int(self.state.get("sync_eta", 0)),
            "sync_busy": bool(self.state.get("sync_busy")),
            "sync_note": self.state.get("sync_note", ""),
            "sync_log": list(self.state.get("sync_log", []))[-10:],
            "last_error": self.state.get("last_error", ""),
            "started_at": self.state.get("started_at", ""),
            "pipeline_busy": bool(self.state.get("pipeline_busy")),
            "pipeline_kind": self.state.get("pipeline_kind", ""),
            "pipeline_item": self.state.get("pipeline_item", ""),
            "pipeline_key": self.state.get("pipeline_key", ""),
            "pipeline_note": self.state.get("pipeline_note", ""),
            "pipeline_retry_eta": int(self.state.get("pipeline_retry_eta", 0)),
            "activity_log": list(self.state.get("activity_log") or [])[-40:],
            "pipeline_log": _tail(PIPELINE_LOG, 30) if ARB_AVAILABLE else [],
            "recovery": {
                "required": bool(self.state.get("recovery_required")),
                "checks": int(self.state.get("recovery_checks", 0)),
                "accounts": int(self.state.get("recovery_accounts", 0)),
            },
            "next_check": None,
            "next_proliv": None,
        }
        if not ARB_AVAILABLE:
            out["import_error"] = ARB_IMPORT_ERROR
            return out
        now = datetime.now()
        with dates_file_lock:
            dates = load_dates(DATES_OF_CHECK)
        nxt = next_scheduled(dates)
        if nxt:
            key, when = nxt
            item_id, slot = (key.split("#", 1) if "#" in key else (key, "E"))
            out["next_check"] = {
                "item_id": item_id,
                "slot": slot,
                "slot_label": slot_label(slot),
                "when": when.strftime(DATE_FMT),
                "left_seconds": int((when - now).total_seconds()),
            }
        pl = next_proliv_scheduled(PROLIV_QUEUE)
        if pl:
            out["next_proliv"] = {
                "item_id": pl[0],
                "run_at_str": _fmt_unix(pl[1]),
                "left_seconds": int(pl[1] - time.time()),
            }
        return out


service = ArbService()


# ── Pydantic ─────────────────────────────────────────────────────────────────
class ItemPayload(BaseModel):
    item: str = ""
    slot: Optional[str] = None
    also_unchecked: bool = False


class BulkPayload(BaseModel):
    text: str = ""
    also_unchecked: bool = False


class ErrKeyPayload(BaseModel):
    err_key: str = ""
    item: str = ""


class TransferPayload(BaseModel):
    item_ids: List[str] = []
    recipient: str = ""


class TransferSettingsPayload(BaseModel):
    auto_enabled: Optional[bool] = None
    interval_minutes: Optional[int] = None
    recipient: Optional[str] = None


class RecoveryPayload(BaseModel):
    action: str


class PurgePayload(BaseModel):
    targets: List[str] = []
    confirmation: str = ""
    disable_service: bool = False


class ReferenceLinePayload(BaseModel):
    list_name: str
    line_index: int
    original: str = ""
    value: str = ""


class ArbToggle(BaseModel):
    enabled: bool


class ArbConfigPayload(BaseModel):
    config: Dict[str, Any]


# ── Router ───────────────────────────────────────────────────────────────────
router = APIRouter(prefix="/api/arb")


def _require_arb() -> None:
    if not ARB_AVAILABLE:
        raise HTTPException(503, f"Модули проверки недоступны: {ARB_IMPORT_ERROR}")


@router.get("/status")
def api_status():
    return service.status()


@router.post("/toggle")
def api_toggle(payload: ArbToggle):
    _require_arb()
    cfg = load_arb_config()
    cfg["enabled"] = payload.enabled
    save_arb_config(cfg)
    if payload.enabled:
        service.start()
    else:
        service.stop()
    return {"ok": True, "running": service.running}


@router.post("/sync-now")
def api_sync_now():
    _require_arb()
    if not service.running:
        raise HTTPException(400, "Сервис проверки не запущен")
    service._sync_now.set()
    return {"ok": True}


@router.get("/config")
def api_get_config():
    _require_arb()
    cfg = load_arb_config()
    cfg["transfer_secret_answer"] = ""
    cfg["transfer_secret_answer_set"] = has_secret(TRANSFER_SECRET)
    return {"ok": True, "config": cfg}


@router.put("/config")
def api_put_config(payload: ArbConfigPayload):
    _require_arb()
    cfg = load_arb_config()
    secret = str(payload.config.get("transfer_secret_answer") or "").strip()
    clear_secret = bool(payload.config.get("transfer_secret_answer_clear"))
    if secret:
        try:
            save_secret(TRANSFER_SECRET, secret)
        except Exception as exc:
            raise HTTPException(500, f"Не удалось защитить секретный ответ: {exc}") from exc
    elif clear_secret:
        save_secret(TRANSFER_SECRET, "")
    for key, value in payload.config.items():
        if key in ARB_DEFAULTS:
            if key == "transfer_recipients":
                clean_recipients = []
                seen = set()
                for row in value if isinstance(value, list) else []:
                    if not isinstance(row, dict):
                        continue
                    name = str(row.get("name") or "").strip()[:80]
                    username = str(row.get("username") or "").strip().lstrip("@")[:80]
                    if not name or not username or username.casefold() in seen:
                        continue
                    seen.add(username.casefold())
                    clean_recipients.append({"name": name, "username": username})
                value = clean_recipients
            elif key == "telegram_bots":
                clean_bots = []
                for index, row in enumerate(value if isinstance(value, list) else []):
                    if not isinstance(row, dict) or len(clean_bots) >= 5:
                        continue
                    token = str(row.get("token") or "").strip()[:256]
                    chat_id = str(row.get("chat_id") or "").strip()[:100]
                    name = str(row.get("name") or f"Бот {index + 1}").strip()[:80]
                    if not token or not chat_id:
                        continue
                    clean_bots.append({
                        "name": name or f"Бот {index + 1}",
                        "token": token,
                        "chat_id": chat_id,
                        "enabled": bool(row.get("enabled", True)),
                    })
                value = clean_bots
            elif key == "proliv_extra_games":
                if not isinstance(value, dict):
                    raise HTTPException(400, "Настройки скрытых игр должны быть объектом")
                value = normalize_extra_games(value)
            elif key == "orders_stop_without_guarantee_enabled":
                value = bool(value)
            elif key == "orders_no_guarantee_page_limit":
                try:
                    value = max(1, min(3, int(value)))
                except (TypeError, ValueError) as exc:
                    raise HTTPException(400, "Лимит страниц без гарантии должен быть от 1 до 3") from exc
            elif key == "proliv_retry_max":
                try:
                    value = max(1, min(10, int(value)))
                except (TypeError, ValueError) as exc:
                    raise HTTPException(400, "Количество попыток пролива должно быть от 1 до 10") from exc
            elif key == "telegram_error_bot_index":
                try:
                    value = max(-1, min(4, int(value)))
                except (TypeError, ValueError) as exc:
                    raise HTTPException(400, "Некорректно выбран Telegram-бот для ошибок") from exc
            elif key in {
                "proliv_after_publish_tag_enabled", "telegram_separate_errors",
            }:
                value = bool(value)
            elif key in {"telegram_notification_template", "telegram_error_template"}:
                value = str(value or "")[:4096]
            cfg[key] = value
    if "telegram_bots" in payload.config:
        bots = cfg.get("telegram_bots") or []
        first = bots[0] if bots else {}
        second = bots[1] if len(bots) > 1 else {}
        cfg.update({
            "telegram_enabled": bool(first and first.get("enabled")),
            "telegram_token": str(first.get("token") or ""),
            "telegram_chat_id": str(first.get("chat_id") or ""),
            "telegram_friend_enabled": bool(second and second.get("enabled")),
            "telegram_friend_token": str(second.get("token") or ""),
            "telegram_friend_chat_id": str(second.get("chat_id") or ""),
        })
    save_arb_config(cfg)
    service.restart_if_running()
    public_cfg = load_arb_config()
    public_cfg["transfer_secret_answer"] = ""
    public_cfg["transfer_secret_answer_set"] = has_secret(TRANSFER_SECRET)
    return {"ok": True, "config": public_cfg, "running": service.running}


def _build_checks_proliv():
    now = datetime.now()
    now_unix = int(time.time())
    with dates_file_lock:
        dates = load_dates(DATES_OF_CHECK)
    with proliv_file_lock:
        proliv_rows = load_proliv_queue(PROLIV_QUEUE)

    checks = []
    for key, when in sorted(dates.items(), key=lambda x: x[1]):
        item_id, slot = (key.split("#", 1) if "#" in key else (key, "E"))
        left = (when - now).total_seconds()
        checks.append({
            "key": key,
            "item_id": item_id,
            "slot": slot,
            "slot_label": slot_label(slot),
            "when": when.strftime("%d-%m-%Y %H:%M:%S"),
            "left_seconds": int(left),
            "overdue": left <= 0,
            "processing": bool(service.state.get("pipeline_busy") and
                               service.state.get("pipeline_kind") == "check" and
                               service.state.get("pipeline_key") == key),
        })

    proliv = []
    for row in proliv_rows:
        run_at = int(row.get("run_at_unix", 0))
        proliv.append({
            "item_id": str(row.get("item_id", "")),
            "run_at_unix": run_at,
            "run_at_str": _fmt_unix(run_at),
            "left_seconds": run_at - now_unix,
            "draft_item_id": row.get("draft_item_id"),
            "manual_review": bool(row.get("manual_review")),
            "blocked_reason": str(row.get("blocked_reason") or ""),
            "retry_count": int(row.get("retry_count", 0) or 0),
            "max_retries": int(row.get("max_retries", 0) or 0),
            "last_error": str(row.get("last_error") or ""),
            "last_error_type": str(row.get("last_error_type") or ""),
            "last_error_label": str(row.get("last_error_label") or ""),
            "last_error_at_unix": int(row.get("last_error_at_unix", 0) or 0),
            "api_response": str(row.get("api_response") or ""),
            "endpoint": str(row.get("endpoint") or ""),
            "http_status": row.get("http_status"),
            "retry_delay_seconds": int(row.get("retry_delay_seconds", 0) or 0),
            "processing": bool(service.state.get("pipeline_busy") and
                               service.state.get("pipeline_kind") == "proliv" and
                               service.state.get("pipeline_item") == str(row.get("item_id", ""))),
        })
    return checks, proliv


def _tracked_guarantees(checks: List[Dict[str, Any]], proliv: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    item_ids = {str(row.get("item_id") or "") for row in [*checks, *proliv]}
    item_ids.discard("")
    return collect_tracked_guarantee_records(_reference_records(GUARANTEE_TXT), item_ids)


@router.get("/data")
def api_data():
    _require_arb()
    checks, proliv = _build_checks_proliv()
    visible_proliv = [row for row in proliv if not row.get("manual_review")]
    resold = load_resold_items(RESOLD_FILE)
    val_errors = _load_actionable_validation_errors()
    operational_errors = [row for row in val_errors if not _is_invalid_account_result(row)]
    invalid_accounts = _collect_invalid_account_rows(val_errors, proliv)
    stats = compute_dashboard_stats(
        checks, proliv, operational_errors,
        valid_history=VALID_HISTORY,
        claim_history=CLAIM_HISTORY,
        proliv_history=PROLIV_HISTORY,
        pipeline_history=PIPELINE_LOG,
    )
    active_ids = {str(row.get("item_id") or "") for row in [*checks, *visible_proliv]}
    active_ids.discard("")
    stats["accounts"] = len(active_ids)
    stats["proliv"] = len(visible_proliv)
    stats["resold"] = len(resold)
    stats["guarantee"] = len(_tracked_guarantees(checks, visible_proliv))
    operational_proliv_errors = [
        row for row in collect_proliv_queue_errors(proliv, PROLIV_HISTORY)
        if not _is_invalid_account_result(row)
    ]
    stats["proliv_errors"] = len(operational_proliv_errors)
    stats["errors"] = len(operational_errors) + len(operational_proliv_errors)
    stats["invalid"] = sum(1 for row in invalid_accounts if row.get("kind") == "invalid")
    stats["kt"] = sum(1 for row in invalid_accounts if row.get("kind") == "kt")
    stats["invalids"] = len(invalid_accounts)
    return {
        "ok": True,
        "stats": stats,
        "checks": checks,
        "proliv": visible_proliv,
        "resold": resold,
    }


@router.get("/heavy")
def api_heavy():
    _require_arb()
    checks, proliv = _build_checks_proliv()
    guarantee_records = _tracked_guarantees(
        checks, [row for row in proliv if not row.get("manual_review")],
    )
    blacklist_records = _reference_records(BLACKLIST_TXT)
    guarantee_lines = [record["value"] for record in guarantee_records]
    blacklist_lines = [record["value"] for record in blacklist_records]
    return {
        "ok": True,
        "valid_history":   _tail(VALID_HISTORY, 200),
        "claim_history":   _tail(CLAIM_HISTORY, 500),
        "pipeline_log":    _tail(PIPELINE_LOG, 200),
        "proliv_history":  _tail(PROLIV_HISTORY, 300),
        "guarantee_items": guarantee_lines,
        "blacklist_items": blacklist_lines[-200:],
        "guarantee_records": guarantee_records,
        "blacklist_records": blacklist_records[-200:],
    }


@router.post("/recover-missed-checks")
def api_recover_missed_checks(payload: RecoveryPayload):
    _require_arb()
    action = payload.action.strip().lower()
    if action not in {"delete", "run_latest"}:
        raise HTTPException(400, "Неизвестный способ обработки пропущенных проверок")

    now = datetime.now()
    # The in-memory snapshot is part of the source of truth.  An older worker
    # could already have consumed a due line while the recovery dialog was
    # open, leaving the dialog with one pending key but the schedule file with
    # none.  Handling the union makes both actions idempotent and clears that
    # stale state instead of returning zero forever.
    with service._recovery_lock:
        pending_keys = set(service._recovery_pending_keys)
    scheduled_keys: set[str] = set()
    with dates_file_lock:
        dates = load_dates(DATES_OF_CHECK)
        file_due_keys = {key for key, when in dates.items() if when <= now}
        due_keys = file_due_keys | pending_keys
        accounts = sorted({key.split("#", 1)[0] for key in due_keys})
        for key in file_due_keys:
            dates.pop(key, None)
        if action == "run_latest":
            run_at = now - timedelta(seconds=5)
            for item_id in accounts:
                key = f"{item_id}#recovery"
                dates[key] = run_at
                scheduled_keys.add(key)
        write_dates(DATES_OF_CHECK, dates)

    service.resolve_recovery(due_keys, scheduled_keys)
    if due_keys:
        if action == "run_latest":
            service._activity(
                f"Пропущенные этапы свёрнуты: по одной проверке для {len(accounts)} аккаунтов",
                "success",
            )
        else:
            service._activity(f"Удалено пропущенных проверок: {len(due_keys)}", "info")
    return {
        "ok": True,
        "action": action,
        "removed_checks": len(due_keys),
        "accounts": len(accounts),
        "scheduled_checks": len(scheduled_keys),
    }


def _maintenance_counts() -> Dict[str, int]:
    with dates_file_lock:
        checks = len(load_dates(DATES_OF_CHECK))
    with proliv_file_lock:
        proliv = len(load_proliv_queue(PROLIV_QUEUE))
    with reference_file_lock:
        references = _count_lines(GUARANTEE_TXT) + _count_lines(BLACKLIST_TXT)
    errors = len(_load_actionable_validation_errors()) + _count_lines(TELEGRAM_ERR_LOG)
    with resold_file_lock:
        resold = len(load_resold_items(RESOLD_FILE))
    with transfer_items_lock:
        transfer = len(load_transferred_items(TRANSFERRED_ITEMS)) + _count_lines(TRANSFER_LOG)
    history = sum(_count_lines(path) for path in (
        VALID_HISTORY, CLAIM_HISTORY, PROLIV_HISTORY, PIPELINE_LOG,
    ))
    return {
        "checks": checks,
        "proliv": proliv,
        "history": history,
        "references": references,
        "errors": errors,
        "transfer": transfer,
        "checked": _count_lines(CHECKED_ITEMS),
        "resold": resold,
    }


@router.get("/maintenance-summary")
def api_maintenance_summary():
    _require_arb()
    counts = _maintenance_counts()
    return {"ok": True, "counts": counts, "total": sum(counts.values()), "running": service.running}


@router.post("/purge-data")
def api_purge_data(payload: PurgePayload):
    _require_arb()
    if payload.confirmation.strip().upper() != "ОЧИСТИТЬ":
        raise HTTPException(400, "Введи слово ОЧИСТИТЬ для подтверждения")
    allowed = {"checks", "proliv", "history", "references", "errors", "transfer", "checked", "resold"}
    targets = {str(value).strip().lower() for value in payload.targets}
    if "all" in targets:
        targets = set(allowed)
    if not targets or not targets.issubset(allowed):
        raise HTTPException(400, "Не выбраны данные для очистки")
    full_cleanup = targets == allowed
    if payload.disable_service and not full_cleanup:
        raise HTTPException(400, "Отключение сервиса доступно только при полной очистке")
    if service.running and not (payload.disable_service and full_cleanup):
        raise HTTPException(409, "Перед выборочной очисткой останови сервис проверки")
    if payload.disable_service:
        service.stop()
        cfg = load_arb_config()
        cfg["enabled"] = False
        save_arb_config(cfg)

    before = _maintenance_counts()
    if "checks" in targets:
        with dates_file_lock:
            write_dates(DATES_OF_CHECK, {})
        service.resolve_recovery(set(service._recovery_pending_keys), set())
    if "proliv" in targets:
        with proliv_file_lock:
            save_proliv_queue(PROLIV_QUEUE, [])
    if "history" in targets:
        with history_file_lock:
            for path in (VALID_HISTORY, CLAIM_HISTORY, PROLIV_HISTORY, PIPELINE_LOG):
                path.write_text("", encoding="utf-8")
        service.state["activity_log"] = []
        service.state["sync_log"] = []
        service.state["last_error"] = ""
    if "references" in targets:
        with reference_file_lock:
            GUARANTEE_TXT.write_text("", encoding="utf-8")
            BLACKLIST_TXT.write_text("", encoding="utf-8")
    if "errors" in targets:
        with valid_errors_file_lock:
            VALIDATION_ERRORS.write_text("[]\n", encoding="utf-8")
            TELEGRAM_ERR_LOG.write_text("", encoding="utf-8")
    if "transfer" in targets:
        with transfer_items_lock:
            TRANSFERRED_ITEMS.write_text("[]\n", encoding="utf-8")
            TRANSFER_LOG.write_text("", encoding="utf-8")
    if "checked" in targets:
        CHECKED_ITEMS.write_text("", encoding="utf-8")
    if "resold" in targets:
        with resold_file_lock:
            RESOLD_FILE.write_text("[]\n", encoding="utf-8")

    service._activity(f"Очищены данные: {', '.join(sorted(targets))}", "info")
    return {
        "ok": True,
        "targets": sorted(targets),
        "deleted": {key: before[key] for key in sorted(targets)},
        "deleted_total": sum(before[key] for key in targets),
        "disabled": bool(payload.disable_service),
    }


@router.post("/activity-log/clear")
def api_clear_activity_log():
    """Clear only the live execution journal, without touching check history."""
    _require_arb()
    with history_file_lock:
        PIPELINE_LOG.parent.mkdir(parents=True, exist_ok=True)
        PIPELINE_LOG.write_text("", encoding="utf-8")
    service.state["activity_log"] = []
    service.state["sync_log"] = []
    service.state["last_error"] = ""
    return {"ok": True}


def _reference_path(name: str) -> Path:
    paths = {"guarantee": GUARANTEE_TXT, "blacklist": BLACKLIST_TXT}
    path = paths.get(name)
    if path is None:
        raise HTTPException(400, "Неизвестный список")
    return path


def _change_reference_line(payload: ReferenceLinePayload, *, delete: bool) -> Dict[str, Any]:
    path = _reference_path(payload.list_name.strip().lower())
    with reference_file_lock:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines() if path.exists() else []
        if payload.line_index < 0 or payload.line_index >= len(lines):
            raise HTTPException(409, "Список уже изменился. Обнови данные и повтори действие.")
        current = lines[payload.line_index].strip()
        if payload.original and current != payload.original.strip():
            raise HTTPException(409, "Эта запись уже была изменена. Обнови список.")
        if delete:
            removed = lines.pop(payload.line_index)
            value = ""
        else:
            value = payload.value.replace("\r", " ").replace("\n", " ").strip()
            if not value:
                raise HTTPException(400, "Запись не может быть пустой")
            if len(value) > 1000:
                raise HTTPException(400, "Запись слишком длинная")
            removed = ""
            lines[payload.line_index] = value
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(("\n".join(lines) + "\n") if lines else "", encoding="utf-8")
        tmp.replace(path)
    return {"ok": True, "deleted": delete, "removed": removed, "value": value}


@router.post("/reference-line/update")
def api_update_reference_line(payload: ReferenceLinePayload):
    _require_arb()
    return _change_reference_line(payload, delete=False)


@router.post("/reference-line/delete")
def api_delete_reference_line(payload: ReferenceLinePayload):
    _require_arb()
    return _change_reference_line(payload, delete=True)


@router.get("/file/{name}")
def api_view_file(name: str):
    _require_arb()
    allowed = {
        "pipeline":       PIPELINE_LOG,
        "valid_history":  VALID_HISTORY,
        "claim_history":  CLAIM_HISTORY,
        "proliv_history": PROLIV_HISTORY,
        "tg_errors":      TELEGRAM_ERR_LOG,
        "guarantee":      GUARANTEE_TXT,
        "blacklist":      BLACKLIST_TXT,
        "transfer_log":   TRANSFER_LOG,
    }
    if name not in allowed:
        raise HTTPException(404, "Неизвестный файл")
    return {"ok": True, "name": name, "lines": _tail(allowed[name], 200)}


def _remove_item_from_reference_files(item_id: str) -> int:
    removed = 0
    pattern = re.compile(rf"(?<!\d){re.escape(item_id)}(?!\d)")
    with reference_file_lock:
        for path in (GUARANTEE_TXT, BLACKLIST_TXT):
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines() if path.exists() else []
            kept = [line for line in lines if not pattern.search(line)]
            removed += len(lines) - len(kept)
            if len(kept) != len(lines):
                temp = path.with_suffix(path.suffix + ".tmp")
                temp.write_text(("\n".join(kept) + "\n") if kept else "", encoding="utf-8")
                temp.replace(path)
    return removed


def _delete_item_everywhere(item_id: str, also_unchecked: bool = False) -> Dict[str, int]:
    if service.state.get("pipeline_busy") and str(service.state.get("pipeline_item")) == item_id:
        raise HTTPException(409, "Аккаунт сейчас обрабатывается. Дождись завершения запроса.")
    removed: Dict[str, int] = {}
    with dates_file_lock:
        dates = load_dates(DATES_OF_CHECK)
        keys_del = [key for key in dates if key == item_id or key.startswith(f"{item_id}#")]
        for key in keys_del:
            dates.pop(key, None)
        write_dates(DATES_OF_CHECK, dates)
    removed["checks"] = len(keys_del)
    service.resolve_recovery(set(keys_del), set())

    with proliv_file_lock:
        queue = load_proliv_queue(PROLIV_QUEUE)
        new_queue = [row for row in queue if str(row.get("item_id")) != item_id]
        save_proliv_queue(PROLIV_QUEUE, new_queue)
    removed["proliv"] = len(queue) - len(new_queue)
    removed["references"] = _remove_item_from_reference_files(item_id)

    with valid_errors_file_lock:
        errors = load_validation_errors(VALIDATION_ERRORS)
        new_errors = [
            row for row in errors
            if str(row.get("item_id") or "") != item_id
            and not str(row.get("err_key") or "").startswith(f"{item_id}#")
        ]
        if len(new_errors) != len(errors):
            save_validation_errors(VALIDATION_ERRORS, new_errors)
    removed["errors"] = len(errors) - len(new_errors)

    with resold_file_lock:
        resold = load_resold_items(RESOLD_FILE)
        new_resold = [
            row for row in resold
            if str(row.get("item_id")) != item_id and str(row.get("new_item_id")) != item_id
        ]
        if len(new_resold) != len(resold):
            RESOLD_FILE.write_text(json.dumps(new_resold, indent=2, ensure_ascii=False), encoding="utf-8")
    removed["resold"] = len(resold) - len(new_resold)

    with transfer_items_lock:
        transferred = load_transferred_items(TRANSFERRED_ITEMS)
        was_transferred = item_id in transferred
        if was_transferred:
            transferred.discard(item_id)
            TRANSFERRED_ITEMS.write_text(json.dumps(sorted(transferred), indent=2, ensure_ascii=False), encoding="utf-8")
    removed["transferred"] = int(was_transferred)

    removed["unchecked"] = 0
    if also_unchecked:
        checked = load_checked_items(CHECKED_ITEMS)
        if item_id in checked:
            checked.discard(item_id)
            CHECKED_ITEMS.write_text(
                (("\n".join(sorted(checked, key=lambda value: int(value) if value.isdigit() else 0)) + "\n") if checked else ""),
                encoding="utf-8",
            )
            removed["unchecked"] = 1
    return removed


@router.post("/delete-all")
def api_delete_all(payload: ItemPayload):
    _require_arb()
    item_id = _parse_item_id(payload.item)
    if not item_id:
        raise HTTPException(400, f"Не удалось извлечь ID из: {payload.item!r}")

    removed = _delete_item_everywhere(item_id, payload.also_unchecked)
    return {"ok": True, "item_id": item_id, "removed": removed}


@router.post("/remove-check")
def api_remove_check(payload: ItemPayload):
    _require_arb()
    item_id = _parse_item_id(payload.item)
    if not item_id:
        raise HTTPException(400, "Неверный item")
    with dates_file_lock:
        dates = load_dates(DATES_OF_CHECK)
        if payload.slot:
            key = f"{item_id}#{payload.slot}"
            removed = 1 if key in dates else 0
            dates.pop(key, None)
        else:
            keys = [k for k in dates if k == item_id or k.startswith(f"{item_id}#")]
            removed = len(keys)
            for k in keys:
                del dates[k]
        write_dates(DATES_OF_CHECK, dates)
    service.resolve_recovery({f"{item_id}#{payload.slot}"} if payload.slot else set(keys), set())
    return {"ok": True, "removed": removed}


@router.post("/remove-proliv")
def api_remove_proliv(payload: ItemPayload):
    _require_arb()
    item_id = _parse_item_id(payload.item)
    if not item_id:
        raise HTTPException(400, "Неверный item")
    with proliv_file_lock:
        queue = load_proliv_queue(PROLIV_QUEUE)
        new_queue = [r for r in queue if str(r.get("item_id")) != item_id]
        removed = len(queue) - len(new_queue)
        save_proliv_queue(PROLIV_QUEUE, new_queue)
    dismiss_validation_error(
        VALIDATION_ERRORS, valid_errors_file_lock, f"proliv#{item_id}",
    )
    return {"ok": True, "removed": removed}


@router.post("/manual-check")
def api_manual_check(payload: ItemPayload):
    _require_arb()
    item_id = _parse_item_id(payload.item)
    if not item_id:
        raise HTTPException(400, "Неверный item")
    overdue = datetime.now() - timedelta(seconds=5)
    with dates_file_lock:
        dates = load_dates(DATES_OF_CHECK)
        requested_slot = str(payload.slot or "").strip()

        # Running a scheduled row manually must consume that exact stage.
        # A separate #manual key would leave P10/P55/P99 behind and make it
        # run for a second time at its original scheduled time.
        target_key = ""
        if requested_slot:
            requested_key = item_id if requested_slot == "E" else f"{item_id}#{requested_slot}"
            if requested_key not in dates:
                raise HTTPException(409, "Этот этап уже выполнен или удалён")
            target_key = requested_key
        else:
            scheduled = [
                key for key in dates
                if (key == item_id or key.startswith(f"{item_id}#"))
                and key != f"{item_id}#manual"
            ]
            if scheduled:
                target_key = min(scheduled, key=lambda key: dates[key])
            else:
                target_key = f"{item_id}#manual"

        if target_key != f"{item_id}#manual":
            dates.pop(f"{item_id}#manual", None)
        dates[target_key] = overdue
        write_dates(DATES_OF_CHECK, dates)
    return {
        "ok": True,
        "item_id": item_id,
        "key": target_key,
        "slot": target_key.split("#", 1)[1] if "#" in target_key else "E",
        "msg": "Этап перенесён в очередь немедленной проверки",
    }


@router.post("/manual-proliv")
def api_manual_proliv(payload: ItemPayload):
    _require_arb()
    item_id = _parse_item_id(payload.item)
    if not item_id:
        raise HTTPException(400, "Неверный item")
    with proliv_file_lock:
        queue = load_proliv_queue(PROLIV_QUEUE)
        queue = [r for r in queue if str(r.get("item_id")) != item_id]
        queue.append({
            "item_id": item_id,
            "run_at_unix": int(time.time()) - 5,
            "manual_requested": True,
            "manual_requested_at_unix": int(time.time()),
        })
        queue.sort(key=lambda r: int(r.get("run_at_unix", 0)))
        save_proliv_queue(PROLIV_QUEUE, queue)
    append_proliv_history(PROLIV_HISTORY, item_id, "retry_reset: manual restart")
    dismiss_validation_error(
        VALIDATION_ERRORS, valid_errors_file_lock, f"proliv#{item_id}",
    )
    return {"ok": True, "item_id": item_id, "msg": "Добавлено в очередь немедленного пролива"}


@router.post("/remove-resold")
def api_remove_resold(payload: ItemPayload):
    _require_arb()
    item_id = _parse_item_id(payload.item)
    if not item_id:
        raise HTTPException(400, "Не распознан item_id")
    with resold_file_lock:
        items = load_resold_items(RESOLD_FILE)
        new_items = [r for r in items if str(r.get("item_id")) != item_id]
        with RESOLD_FILE.open("w", encoding="utf-8") as f:
            json.dump(new_items, f, indent=2, ensure_ascii=False)
    return {"ok": True, "item_id": item_id}


@router.post("/bulk-delete-all")
def api_bulk_delete_all(payload: BulkPayload):
    _require_arb()
    ids = _parse_item_ids_bulk(payload.text)
    if not ids:
        raise HTTPException(400, "Не найдено ни одного ID в тексте")
    results = []
    for item_id in ids:
        removed = _delete_item_everywhere(item_id, payload.also_unchecked)
        results.append({"item_id": item_id, "removed": removed})
    return {"ok": True, "count": len(ids), "results": results}


@router.post("/bulk-manual-check")
def api_bulk_manual_check(payload: BulkPayload):
    _require_arb()
    ids = _parse_item_ids_bulk(payload.text)
    if not ids:
        raise HTTPException(400, "Не найдено ни одного ID")
    overdue = datetime.now() - timedelta(seconds=5)
    with dates_file_lock:
        dates = load_dates(DATES_OF_CHECK)
        for item_id in ids:
            dates[f"{item_id}#manual"] = overdue
        write_dates(DATES_OF_CHECK, dates)
    return {"ok": True, "count": len(ids), "ids": ids}


@router.post("/bulk-manual-proliv")
def api_bulk_manual_proliv(payload: BulkPayload):
    _require_arb()
    ids = _parse_item_ids_bulk(payload.text)
    if not ids:
        raise HTTPException(400, "Не найдено ни одного ID")
    with proliv_file_lock:
        queue = load_proliv_queue(PROLIV_QUEUE)
        now_ts = int(time.time())
        requested = set(ids)
        queue = [r for r in queue if str(r.get("item_id")) not in requested]
        for i, item_id in enumerate(ids):
            queue.append({
                "item_id": item_id,
                "run_at_unix": now_ts - len(ids) + i,
                "manual_requested": True,
                "manual_requested_at_unix": now_ts,
            })
        queue.sort(key=lambda r: int(r.get("run_at_unix", 0)))
        save_proliv_queue(PROLIV_QUEUE, queue)
    return {"ok": True, "count": len(ids), "ids": ids}


@router.get("/errors")
def api_errors():
    _require_arb()
    errors = [
        row for row in _load_actionable_validation_errors()
        if not _is_invalid_account_result(row)
    ]
    _, proliv = _build_checks_proliv()
    normalized = []
    for row in errors:
        entry = dict(row)
        entry.setdefault("source", "validation")
        entry.setdefault("stage", "Проверка на валид")
        entry.setdefault("error_message", entry.get("last_error_type") or "Ошибка проверки")
        entry.setdefault("api_response", entry.get("error_message") or "")
        normalized.append(entry)
    proliv_errors = [
        row for row in collect_proliv_queue_errors(proliv, PROLIV_HISTORY)
        if not _is_invalid_account_result(row)
    ]
    all_errors = [*normalized, *proliv_errors]
    all_errors.sort(key=_error_sort_timestamp, reverse=True)
    return {
        "ok": True,
        "errors": all_errors,
        "validation_errors": len(normalized),
        "proliv_errors": len(proliv_errors),
    }


@router.get("/invalids")
def api_invalids():
    _require_arb()
    _, proliv = _build_checks_proliv()
    accounts = _collect_invalid_account_rows(_load_actionable_validation_errors(), proliv)
    return {
        "ok": True,
        "accounts": accounts,
        "total": len(accounts),
        "invalid": sum(1 for row in accounts if row.get("kind") == "invalid"),
        "kt": sum(1 for row in accounts if row.get("kind") == "kt"),
    }


@router.post("/dismiss-error")
def api_dismiss_error(payload: ErrKeyPayload):
    _require_arb()
    if not payload.err_key.strip():
        raise HTTPException(400, "err_key обязателен")
    changed = dismiss_validation_error(VALIDATION_ERRORS, valid_errors_file_lock, payload.err_key.strip())
    return {"ok": True, "changed": changed}


@router.post("/force-recheck-error")
def api_force_recheck_error(payload: ErrKeyPayload):
    _require_arb()
    err_key = payload.err_key.strip()
    if err_key and "#" in err_key:
        item_id, slot = err_key.split("#", 1)
    else:
        item_id = _parse_item_id(payload.item or err_key)
        slot = "manual"
    if not item_id:
        raise HTTPException(400, "Не удалось определить item_id")

    key = f"{item_id}#{slot}"
    with dates_file_lock:
        dates = load_dates(DATES_OF_CHECK)
        dates[key] = datetime.now() - timedelta(seconds=5)
        write_dates(DATES_OF_CHECK, dates)
    dismiss_validation_error(VALIDATION_ERRORS, valid_errors_file_lock, err_key or key)
    return {"ok": True, "item_id": item_id, "slot": slot,
            "msg": f"Поставлен на немедленную перепроверку (слот {slot})"}


@router.post("/transfer")
def api_transfer(payload: TransferPayload):
    _require_arb()
    item_ids = [str(x).strip() for x in payload.item_ids if str(x).strip()]
    recipient = payload.recipient.strip()
    if not item_ids:
        raise HTTPException(400, "item_ids пустой")
    if not recipient:
        raise HTTPException(400, "recipient не указан")
    cfg = service.config if service.running else load_arb_config()
    allowed = {
        str(row.get("username") or "").strip().lstrip("@").casefold()
        for row in (cfg.get("transfer_recipients") or [])
        if isinstance(row, dict)
    }
    recipient = recipient.lstrip("@")
    if recipient.casefold() not in allowed:
        raise HTTPException(400, "Получатель не найден в менеджере пользователей")
    client = service.get_client()
    results = []
    for iid in item_ids:
        ok, msg = transfer_item(client, cfg, iid, recipient)
        results.append({"item_id": iid, "ok": ok, "msg": msg})
        if ok:
            add_transferred_item(TRANSFERRED_ITEMS, transfer_items_lock, iid)
        append_transfer_log(TRANSFER_LOG, iid, recipient, "ok" if ok else f"fail: {msg}")
    total_ok = sum(1 for r in results if r["ok"])
    return {"ok": True, "total": len(results), "transferred": total_ok, "results": results}


@router.get("/transfer-settings")
def api_get_transfer_settings():
    _require_arb()
    s = load_transfer_settings(TRANSFER_SETTINGS)
    cfg = load_arb_config()
    recipients = cfg.get("transfer_recipients")
    if not isinstance(recipients, list):
        recipients = []
    transferred = sorted(load_transferred_items(TRANSFERRED_ITEMS))
    return {
        "ok": True,
        "auto_enabled":      s.get("auto_enabled", False),
        "interval_minutes":  s.get("interval_minutes", 60),
        "recipient":         s.get("recipient", recipients[0]["username"] if recipients else ""),
        "recipients":        recipients,
        "secret_set":        has_secret(TRANSFER_SECRET),
        "last_auto_run":     s.get("last_auto_run"),
        "transferred_items": transferred,
    }


@router.post("/transfer-settings")
def api_set_transfer_settings(payload: TransferSettingsPayload):
    _require_arb()
    s = load_transfer_settings(TRANSFER_SETTINGS)
    if payload.auto_enabled is not None:
        s["auto_enabled"] = payload.auto_enabled
    if payload.interval_minutes is not None:
        s["interval_minutes"] = max(1, int(payload.interval_minutes))
    if payload.recipient is not None:
        recipient = payload.recipient.strip().lstrip("@")
        if recipient:
            cfg = load_arb_config()
            allowed = {
                str(row.get("username") or "").strip().lstrip("@").casefold()
                for row in (cfg.get("transfer_recipients") or [])
                if isinstance(row, dict)
            }
            if recipient.casefold() not in allowed:
                raise HTTPException(400, "Получатель не найден в менеджере пользователей")
        s["recipient"] = recipient
    save_transfer_settings(TRANSFER_SETTINGS, s)
    return {"ok": True}


def autostart() -> None:
    """Запуск воркеров при старте панели, если сервис был включён."""
    if not ARB_AVAILABLE:
        return
    try:
        cfg = load_arb_config()
        if cfg.get("enabled") and str(cfg.get("token") or "").strip():
            service.start()
    except Exception as exc:
        logger.warning("[ARB] Автозапуск не удался: %s", exc)
