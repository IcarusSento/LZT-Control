import json
import hashlib
import logging
import math
import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

from services.logging_setup import configure_logging
from autoarb.core.secret_store import protect_text, unprotect_text
from autoarb.core.error_policy import is_maintenance_error
from autoarb.core.throttled_client import (
    fail_lzt_api_request,
    get_lzt_api_monitor,
    lzt_token_fingerprint,
    observe_lzt_rate_limit,
    wait_for_lzt_rate_limit,
)

configure_logging()

import arb
from utilities import create_utilities_router

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

APP_NAME            = "LZT Bump Control"
_db_path             = Path(os.getenv("BUMP_DB_PATH", "bump_control.sqlite3"))
# A relative SQLite path used to depend on the directory from which uvicorn was
# launched.  That could silently open a second, empty database and make existing
# profile ids return 404.  Always anchor it to the application directory.
DB_PATH             = _db_path if _db_path.is_absolute() else BASE_DIR / _db_path
REQUEST_TIMEOUT     = int(os.getenv("BUMP_REQUEST_TIMEOUT", "30"))
MAX_REQUEST_ATTEMPTS = 3
RETRYABLE_HTTP_CODES = {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}
CHECK_EVERY_SECONDS = float(os.getenv("BUMP_SCHEDULER_TICK", "3"))
DEFAULT_INTERVAL    = int(os.getenv("BUMP_DEFAULT_INTERVAL_MINUTES", "60"))
DEFAULT_LIMIT       = int(os.getenv("BUMP_DEFAULT_BUMP_LIMIT", "6"))
API_VERSION         = 7

STATIC_DIR = BASE_DIR / "static"
TUTORIAL_DIR = BASE_DIR / "tutorial"
UTC = timezone.utc

_state_lock    = threading.Lock()
_budget_lock   = threading.Lock()
RUNNING_STATES: Dict[int, Dict[str, Any]] = {}
PROFILE_LOCKS:  Dict[int, threading.Lock]  = {}


def _get_lock(pid: int) -> threading.Lock:
    with _state_lock:
        if pid not in PROFILE_LOCKS:
            PROFILE_LOCKS[pid] = threading.Lock()
        return PROFILE_LOCKS[pid]

def _get_rs(pid: int) -> Dict[str, Any]:
    with _state_lock:
        if pid not in RUNNING_STATES:
            RUNNING_STATES[pid] = {"is_running": False, "started_at": None, "trigger": None}
        return dict(RUNNING_STATES[pid])

def _set_rs(pid: int, **kw: Any) -> None:
    with _state_lock:
        if pid not in RUNNING_STATES:
            RUNNING_STATES[pid] = {"is_running": False, "started_at": None, "trigger": None}
        RUNNING_STATES[pid].update(kw)


logger = logging.getLogger("lzt_bump")


def utc_now() -> datetime: return datetime.now(UTC)
def iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.astimezone(UTC).isoformat() if dt else None
def parse_dt(v: Optional[str]) -> Optional[datetime]:
    if not v: return None
    try:
        dt = datetime.fromisoformat(v)
        return (dt if dt.tzinfo else dt.replace(tzinfo=UTC)).astimezone(UTC)
    except ValueError: return None


PROXY_SCHEMES = {"http", "https", "socks5", "socks5h"}


def _protect_db(value: Any) -> str:
    return protect_text(str(value or ""))


def _unprotect_db(value: Any) -> str:
    return unprotect_text(str(value or ""))


def _decrypt_row(row: sqlite3.Row, *secret_fields: str) -> Dict[str, Any]:
    data = dict(row)
    for field in secret_fields:
        data[field] = _unprotect_db(data.get(field, ""))
    return data


def parse_proxy_value(raw: str) -> Dict[str, Any]:
    """Parse URL or ip:port[:login:password] into a normalized proxy."""
    value = (raw or "").strip()
    if not value:
        raise ValueError("Прокси не указан")

    scheme = "http"
    host = ""
    port: Optional[int] = None
    username = ""
    password = ""

    if "://" in value:
        parsed = urlparse(value)
        scheme = (parsed.scheme or "http").lower()
        host = parsed.hostname or ""
        port = parsed.port
        username = unquote(parsed.username or "")
        password = unquote(parsed.password or "")
    elif "@" in value:
        auth, endpoint = value.rsplit("@", 1)
        auth_parts = auth.split(":", 1)
        username = auth_parts[0]
        password = auth_parts[1] if len(auth_parts) > 1 else ""
        host_parts = endpoint.rsplit(":", 1)
        if len(host_parts) == 2:
            host, port_raw = host_parts
            port = int(port_raw)
    else:
        parts = value.split(":")
        if len(parts) >= 2:
            host = parts[0]
            port = int(parts[1])
        if len(parts) >= 3:
            username = parts[2]
        if len(parts) >= 4:
            password = ":".join(parts[3:])

    if scheme not in PROXY_SCHEMES:
        raise ValueError("Поддерживаются HTTP, HTTPS и SOCKS5 прокси")
    if not host or port is None or not 1 <= int(port) <= 65535:
        raise ValueError("Проверь IP/хост и порт прокси")

    auth = ""
    if username or password:
        auth = f"{quote(username, safe='')}:{quote(password, safe='')}@"
    proxy_url = f"{scheme}://{auth}{host}:{int(port)}"
    return {
        "scheme": scheme,
        "host": host,
        "port": int(port),
        "username": username,
        "password": password,
        "proxy_url": proxy_url,
    }


@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS proxies (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT NOT NULL DEFAULT '',
                scheme     TEXT NOT NULL DEFAULT 'http',
                host       TEXT NOT NULL DEFAULT '',
                port       INTEGER NOT NULL DEFAULT 0,
                username   TEXT NOT NULL DEFAULT '',
                password   TEXT NOT NULL DEFAULT '',
                proxy_url  TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tokens (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT NOT NULL DEFAULT '',
                login      TEXT NOT NULL DEFAULT '',
                api_token  TEXT NOT NULL DEFAULT '',
                proxy_id   INTEGER,
                proxy_url  TEXT NOT NULL DEFAULT '',
                color      TEXT NOT NULL DEFAULT '',
                daily_limit INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS profiles (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                name             TEXT    NOT NULL DEFAULT 'Задача',
                token_id         INTEGER,
                api_token        TEXT    NOT NULL DEFAULT '',
                proxy_id         INTEGER,
                proxy_url        TEXT    NOT NULL DEFAULT '',
                proxy_missing    INTEGER NOT NULL DEFAULT 0,
                enabled          INTEGER NOT NULL DEFAULT 0,
                interval_locked  INTEGER NOT NULL DEFAULT 0,
                target_mode      TEXT    NOT NULL DEFAULT 'search',
                target_url       TEXT    NOT NULL DEFAULT '',
                interval_minutes INTEGER NOT NULL DEFAULT 60,
                bump_limit       INTEGER NOT NULL DEFAULT 6,
                last_run_at      TEXT,
                next_run_at      TEXT,
                last_status      TEXT    NOT NULL DEFAULT 'never',
                last_message     TEXT    NOT NULL DEFAULT '',
                updated_at       TEXT    NOT NULL DEFAULT ''
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS run_logs (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id     INTEGER NOT NULL DEFAULT 1,
                started_at     TEXT    NOT NULL,
                finished_at    TEXT    NOT NULL,
                trigger_source TEXT    NOT NULL,
                status         TEXT    NOT NULL,
                bumped_count   INTEGER NOT NULL DEFAULT 0,
                items_found    INTEGER NOT NULL DEFAULT 0,
                http_code      INTEGER,
                account_key    TEXT    NOT NULL DEFAULT '',
                item_ids       TEXT    NOT NULL DEFAULT '[]',
                message        TEXT    NOT NULL DEFAULT ''
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS profile_name_history (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id INTEGER NOT NULL,
                old_name   TEXT NOT NULL,
                new_name   TEXT NOT NULL,
                changed_at TEXT NOT NULL
            )
        """)
        profile_columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(profiles)").fetchall()
        }
        token_columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(tokens)").fetchall()
        }
        run_log_columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(run_logs)").fetchall()
        }
        target_mode_added = "target_mode" not in profile_columns
        token_limit_added = "daily_limit" not in token_columns
        account_key_added = "account_key" not in run_log_columns
        for table, col, defn in [
            ("profiles", "token_id",        "INTEGER"),
            ("profiles", "proxy_id",        "INTEGER"),
            ("profiles", "proxy_missing",   "INTEGER NOT NULL DEFAULT 0"),
            ("profiles", "interval_locked", "INTEGER NOT NULL DEFAULT 0"),
            ("profiles", "target_mode",     "TEXT NOT NULL DEFAULT 'search'"),
            ("tokens",   "proxy_id",        "INTEGER"),
            ("tokens",   "proxy_url",       "TEXT NOT NULL DEFAULT ''"),
            ("tokens",   "color",           "TEXT NOT NULL DEFAULT ''"),
            ("tokens",   "daily_limit",     "INTEGER NOT NULL DEFAULT 0"),
            ("run_logs", "account_key",     "TEXT NOT NULL DEFAULT ''"),
        ]:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
            except sqlite3.OperationalError:
                pass

        # Existing installations did not store a target type. Infer it once
        # during the schema migration; afterwards an empty direct list must
        # remain direct instead of silently becoming a search task.
        if target_mode_added:
            for row in conn.execute("SELECT id,target_url FROM profiles").fetchall():
                inferred = "direct" if extract_direct_ids(row["target_url"]) else "search"
                conn.execute("UPDATE profiles SET target_mode=? WHERE id=?", (inferred, row["id"]))

        # The previous version stored one global budget. Preserve that value
        # for every existing LZT account, then manage it independently.
        if token_limit_added:
            legacy = conn.execute(
                "SELECT value FROM settings WHERE key='daily_limit'"
            ).fetchone()
            try:
                legacy_limit = max(0, int(legacy["value"] or 0)) if legacy else 0
            except (TypeError, ValueError):
                legacy_limit = 0
            if legacy_limit:
                conn.execute("UPDATE tokens SET daily_limit=?", (legacy_limit,))

        # Persist the owner on every history row. This keeps rolling usage
        # correct even if a task is later moved to another token/account.
        if account_key_added:
            conn.execute("""
                UPDATE run_logs
                SET account_key = COALESCE(
                    (SELECT 'token:' || p.token_id FROM profiles p
                     WHERE p.id=run_logs.profile_id AND p.token_id IS NOT NULL),
                    'profile:' || profile_id
                )
                WHERE COALESCE(account_key,'')=''
            """)

        # Import legacy inline proxies into the new reusable proxy directory.
        sources = conn.execute("""
            SELECT proxy_url, name FROM tokens
            WHERE COALESCE(proxy_url,'')!='' AND proxy_id IS NULL
            UNION ALL
            SELECT proxy_url, name FROM profiles
            WHERE COALESCE(proxy_url,'')!='' AND proxy_id IS NULL
        """).fetchall()
        for source in sources:
            stored_proxy = source["proxy_url"]
            raw_proxy = _unprotect_db(stored_proxy)
            try:
                parsed_proxy = parse_proxy_value(raw_proxy)
            except (ValueError, TypeError):
                continue
            existing = next((
                row for row in conn.execute("SELECT id,proxy_url FROM proxies").fetchall()
                if _unprotect_db(row["proxy_url"]) == parsed_proxy["proxy_url"]
            ), None)
            if existing:
                proxy_id = int(existing["id"])
            else:
                label = (source["name"] or "Импортированный").strip()
                cur = conn.execute("""
                    INSERT INTO proxies(name,scheme,host,port,username,password,proxy_url,created_at,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?)
                """, (
                    f"{label} · прокси", parsed_proxy["scheme"], parsed_proxy["host"],
                    parsed_proxy["port"], parsed_proxy["username"], _protect_db(parsed_proxy["password"]),
                    _protect_db(parsed_proxy["proxy_url"]), iso(utc_now()), iso(utc_now()),
                ))
                proxy_id = int(cur.lastrowid)
            for table in ("tokens", "profiles"):
                conn.execute(
                    f"UPDATE {table} SET proxy_id=?, proxy_url=? WHERE proxy_url=? AND proxy_id IS NULL",
                    (proxy_id, _protect_db(parsed_proxy["proxy_url"]), stored_proxy),
                )

        # One-time transparent migration: API tokens and proxy credentials are
        # tied to the current Windows user through DPAPI instead of remaining
        # as readable text in SQLite.
        for table, fields in {
            "proxies": ("password", "proxy_url"),
            "tokens": ("api_token", "proxy_url"),
            "profiles": ("api_token", "proxy_url"),
        }.items():
            rows = conn.execute(f"SELECT id,{','.join(fields)} FROM {table}").fetchall()
            for row in rows:
                updates = {field: _protect_db(row[field]) for field in fields if row[field]}
                if updates:
                    sets = ",".join(f"{field}=?" for field in updates)
                    conn.execute(
                        f"UPDATE {table} SET {sets} WHERE id=?",
                        [*updates.values(), row["id"]],
                    )


# ── Settings ────────────────────────────────────────────────────────────────
def get_setting(key: str, default: str = "") -> str:
    with db() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default

def get_all_settings() -> Dict[str, str]:
    with db() as conn:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
    return {r["key"]: r["value"] for r in rows}

def set_setting(key: str, value: str) -> None:
    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value)
        )


# ── Today bumps ─────────────────────────────────────────────────────────────
def get_today_bumps() -> int:
    since = iso(utc_now() - timedelta(hours=24))
    with db() as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(bumped_count),0) AS t FROM run_logs WHERE started_at>=? AND status IN ('ok','partial')",
            (since,)
        ).fetchone()
    return int(row["t"])

def get_today_bumps_per_profile() -> Dict[int, int]:
    since = iso(utc_now() - timedelta(hours=24))
    with db() as conn:
        rows = conn.execute(
            "SELECT profile_id, COALESCE(SUM(bumped_count),0) AS t FROM run_logs WHERE started_at>=? AND status IN ('ok','partial') GROUP BY profile_id",
            (since,)
        ).fetchall()
    return {r["profile_id"]: int(r["t"]) for r in rows}


def profile_account_key(profile: Dict[str, Any]) -> str:
    """Stable budget owner for a task without exposing its API token."""
    if profile.get("token_id") is not None:
        return f"token:{int(profile['token_id'])}"
    api_token = str(profile.get("api_token") or "").strip()
    if api_token:
        digest = hashlib.sha256(api_token.encode("utf-8")).hexdigest()[:20]
        return f"manual:{digest}"
    return f"profile:{int(profile['id'])}"


def profile_daily_limit(profile: Dict[str, Any]) -> int:
    """Return the independent 24-hour limit of the task's LZT account."""
    if profile.get("token_id") is not None:
        token = get_token(int(profile["token_id"]))
        return max(0, int(token.get("daily_limit", 0))) if token else 0
    # Legacy inline-token tasks have no account record. Keep the old setting as
    # a compatibility fallback, but do not mix their usage with saved tokens.
    return max(0, int(get_setting("daily_limit", "0")))


def get_rolling_limit_status(daily_limit: Optional[int] = None,
                             account_key: Optional[str] = None) -> Dict[str, Any]:
    """Return persisted rolling 24-hour usage and its next exact release."""
    limit = daily_limit
    if limit is None:
        limit = int(get_setting("daily_limit", "0"))
    since = iso(utc_now() - timedelta(hours=24))
    where_owner = " AND account_key=?" if account_key else ""
    params: List[Any] = [since]
    if account_key:
        params.append(account_key)
    with db() as conn:
        rows = conn.execute(
            f"""
            SELECT started_at, bumped_count
            FROM run_logs
            WHERE started_at>=? AND status IN ('ok','partial') AND bumped_count>0
              {where_owner}
            ORDER BY started_at ASC, id ASC
            """,
            params,
        ).fetchall()

    total = sum(int(r["bumped_count"]) for r in rows)
    next_release_at: Optional[str] = None
    next_release_count = 0
    if rows:
        oldest = parse_dt(rows[0]["started_at"])
        if oldest:
            next_release_at = iso(oldest + timedelta(hours=24))
            next_release_count = sum(
                int(r["bumped_count"])
                for r in rows
                if parse_dt(r["started_at"]) == oldest
            )

    return {
        "today_bumps": total,
        "remaining": max(0, limit - total) if limit and limit > 0 else None,
        "limit_reached": bool(limit and limit > 0 and total >= limit),
        "next_release_at": next_release_at,
        "next_release_count": next_release_count,
    }


def get_account_budget_status(account_key: str, daily_limit: int) -> Dict[str, Any]:
    """Rolling usage plus a protected remainder for interval-locked tasks."""
    rolling = get_rolling_limit_status(daily_limit, account_key)
    locked = [
        profile for profile in get_all_profiles()
        if profile["enabled"] and profile["interval_locked"]
        and profile_account_key(profile) == account_key
    ]
    reserved_total = min(
        max(0, int(daily_limit)),
        max(0, math.ceil(sum(estimated_profile_bpd(profile) for profile in locked))),
    )
    locked_ids = [profile["id"] for profile in locked]
    locked_used = 0
    if locked_ids:
        since = iso(utc_now() - timedelta(hours=24))
        placeholders = ",".join("?" for _ in locked_ids)
        with db() as conn:
            row = conn.execute(
                f"""
                SELECT COALESCE(SUM(bumped_count),0) AS t FROM run_logs
                WHERE started_at>=? AND status IN ('ok','partial')
                  AND account_key=? AND profile_id IN ({placeholders})
                """,
                [since, account_key, *locked_ids],
            ).fetchone()
        locked_used = int(row["t"])
    reserved_remaining = max(0, reserved_total - locked_used)
    global_remaining = int(rolling["remaining"] or 0) if daily_limit > 0 else None
    return {
        **rolling,
        "reserved_total": reserved_total,
        "reserved_remaining": reserved_remaining,
        "locked_used": locked_used,
        "auto_remaining": (
            max(0, global_remaining - reserved_remaining)
            if global_remaining is not None else None
        ),
    }


# ── Auto-distribute ─────────────────────────────────────────────────────────
DISTRIBUTION_MODES: Dict[str, Dict[str, Any]] = {
    "frequent": {
        "label": "Часто", "strategy": "frequent", "bump_limit": 1,
        "description": "По 1 объявлению из каждой задачи — максимально частые появления",
    },
    "balanced": {
        "label": "Умеренно", "strategy": "moderate", "bump_limit": 3,
        "description": "По несколько объявлений за запуск со средним интервалом",
    },
    "batch": {
        "label": "Пачками", "strategy": "hourly", "bump_limit": 6,
        "description": "Рассчитанная пачка из каждой задачи строго каждые 60 минут",
    },
}
DIRECT_ITEM_COOLDOWN_MINUTES = 60


def extract_direct_ids(raw: str) -> List[int]:
    """Return unique direct LZT item ids, or [] when the target is a search URL."""
    parts = [part.strip() for part in re.split(r"[\s,]+", raw or "") if part.strip()]
    if not parts:
        return []
    ids: List[int] = []
    for part in parts:
        if re.fullmatch(r"\d+", part):
            item_id = int(part)
        else:
            match = re.search(r"(?:lzt|lolz)\.market/(\d+)/?$", part, re.IGNORECASE)
            if not match:
                return []
            item_id = int(match.group(1))
        if item_id not in ids:
            ids.append(item_id)
    return ids


def normalize_target_mode(value: Any, target_url: str = "") -> str:
    mode = str(value or "").strip().lower()
    if mode in {"search", "direct"}:
        return mode
    return "direct" if extract_direct_ids(target_url) else "search"


def profile_target_mode(profile: Dict[str, Any]) -> str:
    return normalize_target_mode(profile.get("target_mode"), profile.get("target_url", ""))


def normalize_profile_target(mode: str, target_url: str, bump_limit: int) -> tuple[str, int]:
    """Validate a task target and normalize direct links to one item per line."""
    raw = str(target_url or "").strip()
    if not raw:
        raise HTTPException(400, "Укажи ссылку с фильтром или точечные аккаунты")
    target_mode = normalize_target_mode(mode, raw)
    direct_ids = extract_direct_ids(raw)
    if target_mode == "direct":
        if not direct_ids:
            raise HTTPException(
                400,
                "Для точечного режима добавь ID или ссылки на отдельные аккаунты — по одной на строку",
            )
        normalized = "\n".join(f"https://lzt.market/{item_id}/" for item_id in direct_ids)
        return normalized, min(max(1, int(bump_limit)), len(direct_ids))
    if direct_ids:
        raise HTTPException(
            400,
            "Это ссылки на отдельные аккаунты. Переключи тип задачи на «Точечные аккаунты»",
        )
    if not raw.lower().startswith(("http://", "https://", "?")):
        raise HTTPException(400, "Для режима поиска укажи корректную ссылку с фильтрами")
    return raw, max(1, int(bump_limit))


def remove_direct_item_ids(raw: str, item_ids: List[int]) -> tuple[str, List[int]]:
    """Remove unavailable IDs from a direct target and return (new target, removed)."""
    current = extract_direct_ids(raw)
    remove = {int(item_id) for item_id in item_ids}
    removed = [item_id for item_id in current if item_id in remove]
    remaining = [item_id for item_id in current if item_id not in remove]
    return "\n".join(f"https://lzt.market/{item_id}/" for item_id in remaining), removed


def prune_unavailable_direct_items(pid: int, item_ids: List[int]) -> tuple[List[int], bool]:
    """Remove sold/unavailable items from the latest saved direct task state."""
    if not item_ids:
        return [], False
    current = get_profile(pid)
    if profile_target_mode(current) != "direct":
        return [], False
    target_url, removed = remove_direct_item_ids(current.get("target_url", ""), item_ids)
    if not removed:
        return [], False
    disabled = not bool(extract_direct_ids(target_url))
    fields: Dict[str, Any] = {"target_url": target_url}
    if disabled:
        fields.update(enabled=0, next_run_at=None)
    update_profile(pid, **fields)
    return removed, disabled


def estimated_profile_bpd(profile: Dict[str, Any], *, interval: Optional[int] = None,
                          bump_limit: Optional[int] = None) -> float:
    """Estimate bumps/24h without exceeding the hourly capacity of direct ids."""
    interval_value = max(1, int(interval or profile["interval_minutes"]))
    batch_value = max(1, int(bump_limit or profile["bump_limit"]))
    direct_ids = extract_direct_ids(profile.get("target_url", ""))
    if direct_ids:
        batch_value = min(batch_value, len(direct_ids))
        planned = (1440.0 / interval_value) * batch_value
        capacity = len(direct_ids) * (1440.0 / DIRECT_ITEM_COOLDOWN_MINUTES)
        return min(planned, capacity)
    return (1440.0 / interval_value) * batch_value


def _task_distribution_plan(profile: Dict[str, Any], target_bpd: float,
                            mode_config: Dict[str, Any]) -> Dict[str, Any]:
    """Build a plan whose behaviour strictly matches the selected mode."""
    direct_ids = extract_direct_ids(profile.get("target_url", ""))
    max_batch = len(direct_ids) if direct_ids else 100
    max_batch = max(1, min(100, max_batch))
    target = max(0.0, float(target_bpd))
    strategy = str(mode_config.get("strategy") or "moderate")

    if strategy == "frequent":
        batch = 1
        interval = 1440 if target <= 0 else max(1, min(1440, math.floor(1440.0 / target)))
    elif strategy == "hourly":
        interval = 60
        batch = 1 if target <= 0 else max(1, math.ceil(target / 24.0))
        batch = min(max_batch, batch)
    else:
        batch = min(max_batch, 3)
        interval = (
            1440 if target <= 0
            else max(1, min(1440, math.floor((1440.0 * batch) / target)))
        )

    cooldown_limited = False
    if direct_ids:
        safe_interval = math.ceil(
            DIRECT_ITEM_COOLDOWN_MINUTES * batch / len(direct_ids)
        )
        cooldown_limited = interval < safe_interval
        interval = max(interval, safe_interval)

    scheduled_bpd = estimated_profile_bpd(
        profile, interval=interval, bump_limit=batch,
    )
    # A rounded cadence may have slightly more capacity than the account pool.
    # The rolling hard limit stops the excess; forecast only counts usable quota.
    usable_bpd = min(target, scheduled_bpd)
    return {
        "interval_minutes": interval,
        "bump_limit": batch,
        "bumps_per_day": usable_bpd,
        "scheduled_bpd": scheduled_bpd,
        "direct_item_count": len(direct_ids),
        "cooldown_limited": cooldown_limited,
    }


def auto_distribute_intervals(daily_limit: int, mode: Optional[str] = None,
                              *, apply_changes: bool = True,
                              account_key: Optional[str] = None) -> Dict[str, Any]:
    """Distribute one LZT account's budget only among that account's tasks."""
    if daily_limit <= 0:
        return {"ok": False, "error": "Лимит аккаунта не установлен", "updates": []}

    mode = mode or get_setting("distribution_mode", "batch")
    if mode not in DISTRIBUTION_MODES:
        mode = "batch"
    mode_config = DISTRIBUTION_MODES[mode]

    profiles = [
        p for p in get_all_profiles()
        if p["enabled"] and (account_key is None or profile_account_key(p) == account_key)
    ]
    locked = [p for p in profiles if p["interval_locked"]]
    unlocked = [p for p in profiles if not p["interval_locked"]]
    locked_bpd = sum(estimated_profile_bpd(p) for p in locked)
    remaining = max(0.0, float(daily_limit) - locked_bpd)
    locked_tasks = [{
        "id": p["id"], "name": p["name"],
        "bumps_per_day": round(estimated_profile_bpd(p), 1),
        "interval_minutes": p["interval_minutes"], "bump_limit": p["bump_limit"],
    } for p in locked]
    base = {
        "daily_limit": daily_limit,
        "distribution_mode": mode,
        "mode_label": mode_config["label"],
        "locked_bpd": round(locked_bpd, 1),
        "reserved_bpd": round(locked_bpd, 1),
        "available_bpd": round(remaining, 1),
        "locked_tasks": locked_tasks,
        "account_key": account_key,
        "applied": apply_changes,
    }

    if not profiles:
        return {
            **base, "ok": True, "updates": [],
            "note": "Нет активных задач",
            "total_est": 0, "unused_bpd": float(daily_limit),
        }

    if locked_bpd > daily_limit + 1e-9:
        return {
            **base, "ok": False, "updates": [],
            "warning": (
                f"Зафиксированные задачи резервируют {locked_bpd:.1f}/24ч — "
                f"это больше лимита аккаунта {daily_limit}. Их интервалы не изменены."
            ),
            "total_est": round(locked_bpd, 1), "unused_bpd": 0,
        }
    if not unlocked:
        return {
            **base, "ok": True, "updates": [],
            "note": "Все активные задачи этого аккаунта зафиксированы",
            "total_est": round(locked_bpd, 1),
            "unused_bpd": round(max(0.0, daily_limit - locked_bpd), 1),
        }
    if remaining <= 1e-9:
        return {
            **base, "ok": False, "updates": [],
            "warning": "Весь лимит аккаунта зарезервирован зафиксированными задачами.",
            "total_est": round(locked_bpd, 1), "unused_bpd": 0,
        }

    # Fair water-filling: a short direct list can consume at most 24 bumps per
    # item/day; any unused part of its share is handed to the other tasks.
    specs: List[Dict[str, Any]] = []
    for profile in unlocked:
        direct_ids = extract_direct_ids(profile.get("target_url", ""))
        capacity = (
            len(direct_ids) * (1440.0 / DIRECT_ITEM_COOLDOWN_MINUTES)
            if direct_ids else float("inf")
        )
        specs.append({"profile": profile, "capacity": capacity})
    allocations: Dict[int, float] = {}
    pending = list(specs)
    pool = remaining
    while pending:
        share = pool / len(pending)
        capped = [spec for spec in pending if spec["capacity"] < share - 1e-9]
        if not capped:
            for spec in pending:
                allocations[spec["profile"]["id"]] = share
            break
        for spec in capped:
            allocations[spec["profile"]["id"]] = max(0.0, spec["capacity"])
            pool -= max(0.0, spec["capacity"])
            pending.remove(spec)

    updates: List[Dict[str, Any]] = []
    for spec in specs:
        profile = spec["profile"]
        plan = _task_distribution_plan(
            profile, allocations.get(profile["id"], 0.0), mode_config
        )
        changed = (
            plan["interval_minutes"] != profile["interval_minutes"]
            or plan["bump_limit"] != profile["bump_limit"]
        )
        if apply_changes:
            update_profile(
                profile["id"], interval_minutes=plan["interval_minutes"],
                bump_limit=plan["bump_limit"],
            )
            logger.info(
                "[Smart] [%s] [P%d] %s → %d шт. каждые %d мин (%.1f/24ч)",
                account_key or "legacy", profile["id"], profile["name"],
                plan["bump_limit"], plan["interval_minutes"], plan["bumps_per_day"],
            )
        updates.append({
            "id": profile["id"], "name": profile["name"], **plan,
            "target_bpd": round(allocations.get(profile["id"], 0.0), 1),
            "changed": changed,
        })

    unlocked_bpd = sum(float(update["bumps_per_day"]) for update in updates)
    total_est = locked_bpd + unlocked_bpd
    return {
        **base, "ok": True, "updates": updates,
        "unlocked_bpd": round(unlocked_bpd, 1),
        "total_est": round(total_est, 1),
        "unused_bpd": round(max(0.0, daily_limit - total_est), 1),
    }


def auto_distribute_all_accounts(mode: Optional[str] = None,
                                 *, apply_changes: bool = True) -> Dict[str, Any]:
    """Preview or apply one mode to every saved LZT account at once."""
    mode = mode or get_setting("distribution_mode", "batch")
    if mode not in DISTRIBUTION_MODES:
        mode = "batch"
    accounts: List[Dict[str, Any]] = []
    for token in get_all_tokens():
        limit = max(0, int(token.get("daily_limit", 0)))
        if limit <= 0:
            continue
        token_id = int(token["id"])
        account_key = f"token:{token_id}"
        result = auto_distribute_intervals(
            limit, mode, apply_changes=apply_changes, account_key=account_key,
        )
        result.update(
            token_id=token_id,
            account_name=token.get("name") or f"Аккаунт {token_id}",
            account_color=token.get("color") or "",
        )
        accounts.append(result)

    if not accounts:
        return {
            "ok": False, "accounts": [], "updates": [],
            "error": "Ни у одного LZT-аккаунта не установлен лимит",
            "distribution_mode": mode,
            "mode_label": DISTRIBUTION_MODES[mode]["label"],
            "applied": apply_changes,
        }

    updates = [
        {**update, "token_id": account["token_id"],
         "account_name": account["account_name"]}
        for account in accounts for update in account.get("updates", [])
    ]
    warnings = [
        f"{account['account_name']}: {account['warning']}"
        for account in accounts if account.get("warning")
    ]
    return {
        "ok": True,
        "accounts": accounts,
        "updates": updates,
        "warnings": warnings,
        "warning": " ".join(warnings),
        "daily_limit": sum(int(account.get("daily_limit", 0)) for account in accounts),
        "locked_bpd": round(sum(float(account.get("locked_bpd", 0)) for account in accounts), 1),
        "total_est": round(sum(float(account.get("total_est", 0)) for account in accounts), 1),
        "unused_bpd": round(sum(float(account.get("unused_bpd", 0)) for account in accounts), 1),
        "distribution_mode": mode,
        "mode_label": DISTRIBUTION_MODES[mode]["label"],
        "applied": apply_changes,
    }


# ── Proxies CRUD ────────────────────────────────────────────────────────────
def get_all_proxies() -> List[Dict[str, Any]]:
    with db() as conn:
        rows = conn.execute("""
            SELECT p.*,
                   (SELECT COUNT(*) FROM profiles pr WHERE pr.proxy_id=p.id) AS profile_count,
                   (SELECT COUNT(*) FROM tokens t WHERE t.proxy_id=p.id) AS token_count
            FROM proxies p ORDER BY LOWER(p.name), p.id
        """).fetchall()
    return [_decrypt_row(r, "password", "proxy_url") for r in rows]


def get_proxy(proxy_id: int) -> Optional[Dict[str, Any]]:
    with db() as conn:
        row = conn.execute("SELECT * FROM proxies WHERE id=?", (proxy_id,)).fetchone()
    return _decrypt_row(row, "password", "proxy_url") if row else None


def create_proxy(name: str, raw: str = "", **fields: Any) -> Dict[str, Any]:
    parsed = parse_proxy_value(raw) if raw else parse_proxy_value(
        f"{fields.get('scheme', 'http')}://"
        f"{quote(str(fields.get('username', '')), safe='')}:{quote(str(fields.get('password', '')), safe='')}@"
        f"{fields.get('host', '')}:{fields.get('port', '')}"
    )
    now = iso(utc_now())
    with db() as conn:
        cur = conn.execute("""
            INSERT INTO proxies(name,scheme,host,port,username,password,proxy_url,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?)
        """, (
            name.strip() or parsed["host"], parsed["scheme"], parsed["host"], parsed["port"],
            parsed["username"], _protect_db(parsed["password"]), _protect_db(parsed["proxy_url"]), now, now,
        ))
        row = conn.execute("SELECT * FROM proxies WHERE id=?", (cur.lastrowid,)).fetchone()
    return _decrypt_row(row, "password", "proxy_url")


def update_proxy(proxy_id: int, name: str, raw: str = "", **fields: Any) -> Optional[Dict[str, Any]]:
    current = get_proxy(proxy_id)
    if not current:
        return None
    parsed = parse_proxy_value(raw) if raw else parse_proxy_value(
        f"{fields.get('scheme', current['scheme'])}://"
        f"{quote(str(fields.get('username', current['username'])), safe='')}:{quote(str(fields.get('password', current['password'])), safe='')}@"
        f"{fields.get('host', current['host'])}:{fields.get('port', current['port'])}"
    )
    with db() as conn:
        conn.execute("""
            UPDATE proxies
            SET name=?,scheme=?,host=?,port=?,username=?,password=?,proxy_url=?,updated_at=?
            WHERE id=?
        """, (
            name.strip() or parsed["host"], parsed["scheme"], parsed["host"], parsed["port"],
            parsed["username"], _protect_db(parsed["password"]), _protect_db(parsed["proxy_url"]), iso(utc_now()), proxy_id,
        ))
        protected_proxy = _protect_db(parsed["proxy_url"])
        conn.execute("UPDATE profiles SET proxy_url=? WHERE proxy_id=?", (protected_proxy, proxy_id))
        conn.execute("UPDATE tokens SET proxy_url=? WHERE proxy_id=?", (protected_proxy, proxy_id))
        row = conn.execute("SELECT * FROM proxies WHERE id=?", (proxy_id,)).fetchone()
    return _decrypt_row(row, "password", "proxy_url")


def delete_proxy(proxy_id: int) -> Optional[int]:
    with db() as conn:
        exists = conn.execute("SELECT id,name FROM proxies WHERE id=?", (proxy_id,)).fetchone()
        if not exists:
            return None
        affected = conn.execute(
            "SELECT id,enabled FROM profiles WHERE proxy_id=?", (proxy_id,)
        ).fetchall()
        disabled_count = sum(1 for row in affected if bool(row["enabled"]))
        conn.execute("""
            UPDATE profiles
            SET enabled=0, next_run_at=NULL, proxy_id=NULL, proxy_url='', proxy_missing=1,
                last_status='waiting',
                last_message='Задача отключена: назначенный прокси удалён из справочника',
                updated_at=?
            WHERE proxy_id=?
        """, (iso(utc_now()), proxy_id))
        conn.execute("UPDATE tokens SET proxy_id=NULL, proxy_url='' WHERE proxy_id=?", (proxy_id,))
        conn.execute("DELETE FROM proxies WHERE id=?", (proxy_id,))
    if disabled_count:
        logger.warning(
            "Прокси [%d] %s удалён: безопасно отключено задач — %d",
            proxy_id, exists["name"], disabled_count,
        )
    return disabled_count


def resolve_proxy_choice(proxy_id: Optional[int], raw: str = "") -> tuple[Optional[int], str]:
    if proxy_id is not None:
        saved = get_proxy(proxy_id)
        if not saved:
            raise HTTPException(400, "Выбранный прокси уже удалён")
        return proxy_id, saved["proxy_url"]
    if raw.strip():
        try:
            return None, parse_proxy_value(raw)["proxy_url"]
        except (ValueError, TypeError) as exc:
            raise HTTPException(400, str(exc)) from exc
    return None, ""


# ── Tokens CRUD ─────────────────────────────────────────────────────────────
def _mask(t: str) -> str:
    if not t: return ""
    return "••••" + (t[-4:] if len(t) >= 4 else t)

def get_all_tokens() -> List[Dict]:
    with db() as conn:
        rows = conn.execute("SELECT * FROM tokens ORDER BY id").fetchall()
    return [_decrypt_row(r, "api_token", "proxy_url") for r in rows]

def get_token(tid: int) -> Optional[Dict]:
    with db() as conn:
        row = conn.execute("SELECT * FROM tokens WHERE id=?", (tid,)).fetchone()
    return _decrypt_row(row, "api_token", "proxy_url") if row else None

def create_token(name: str, login: str, api_token: str, proxy_url: str = "", color: str = "",
                 proxy_id: Optional[int] = None, daily_limit: int = 0) -> Dict:
    with db() as conn:
        cur = conn.execute(
            "INSERT INTO tokens (name,login,api_token,proxy_id,proxy_url,color,daily_limit,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (name, login, _protect_db(api_token), proxy_id, _protect_db(proxy_url), color,
             max(0, int(daily_limit)), iso(utc_now()))
        )
        row = conn.execute("SELECT * FROM tokens WHERE id=?", (cur.lastrowid,)).fetchone()
    return _decrypt_row(row, "api_token", "proxy_url")

def update_token(tid: int, **fields: Any) -> Optional[Dict]:
    if not fields: return get_token(tid)
    for secret_field in ("api_token", "proxy_url"):
        if secret_field in fields:
            fields[secret_field] = _protect_db(fields[secret_field])
    sets = ", ".join(f"{k}=?" for k in fields)
    with db() as conn:
        conn.execute(f"UPDATE tokens SET {sets} WHERE id=?", list(fields.values()) + [tid])
        row = conn.execute("SELECT * FROM tokens WHERE id=?", (tid,)).fetchone()
    return _decrypt_row(row, "api_token", "proxy_url") if row else None

def delete_token(tid: int) -> None:
    with db() as conn:
        conn.execute("DELETE FROM tokens WHERE id=?", (tid,))
        conn.execute("UPDATE profiles SET token_id=NULL WHERE token_id=?", (tid,))


# ── Profiles CRUD ───────────────────────────────────────────────────────────
def _profile_row(pid: int) -> sqlite3.Row:
    with db() as conn:
        row = conn.execute("SELECT * FROM profiles WHERE id=?", (pid,)).fetchone()
    if not row: raise HTTPException(404, f"Профиль {pid} не найден")
    return row

def _profile_dict(row: sqlite3.Row) -> Dict[str, Any]:
    # A task may use only a proxy that still exists in the shared proxy
    # directory.  Old builds could leave an inline proxy_url behind while the
    # visible proxy_id was empty, which made the UI show "no proxy" but the
    # runner still connect through that stale address.
    proxy_id = row["proxy_id"]
    return {
        "id":               int(row["id"]),
        "name":             row["name"],
        "token_id":         row["token_id"],
        "api_token":        _unprotect_db(row["api_token"]),
        "proxy_id":         proxy_id,
        "proxy_url":        _unprotect_db(row["proxy_url"]) if proxy_id is not None else "",
        "proxy_missing":    bool(row["proxy_missing"]),
        "enabled":          bool(row["enabled"]),
        "interval_locked":  bool(row["interval_locked"]),
        "target_mode":      normalize_target_mode(row["target_mode"], row["target_url"]),
        "target_url":       row["target_url"],
        "interval_minutes": int(row["interval_minutes"]),
        "bump_limit":       int(row["bump_limit"]),
        "last_run_at":      row["last_run_at"],
        "next_run_at":      row["next_run_at"],
        "last_status":      row["last_status"],
        "last_message":     row["last_message"],
        "updated_at":       row["updated_at"],
    }

def get_profile(pid: int) -> Dict[str, Any]: return _profile_dict(_profile_row(pid))
def get_all_profiles() -> List[Dict[str, Any]]:
    with db() as conn:
        rows = conn.execute("SELECT * FROM profiles ORDER BY id").fetchall()
    return [_profile_dict(r) for r in rows]

def create_profile(**f: Any) -> Dict[str, Any]:
    with db() as conn:
        cur = conn.execute("""
            INSERT INTO profiles
                (name, token_id, api_token, proxy_id, proxy_url, enabled, interval_locked,
                 target_mode, target_url, interval_minutes, bump_limit, last_status, updated_at)
            VALUES (?,?,?,?,?,0,?,?,?,?,?,'never',?)
        """, (f.get("name","Новая задача"), f.get("token_id"), _protect_db(f.get("api_token","")),
              f.get("proxy_id"), _protect_db(f.get("proxy_url","")), int(f.get("interval_locked", False)),
              normalize_target_mode(f.get("target_mode"), f.get("target_url", "")),
              f.get("target_url",""), f.get("interval_minutes", DEFAULT_INTERVAL),
              f.get("bump_limit", DEFAULT_LIMIT), iso(utc_now())))
        row = conn.execute("SELECT * FROM profiles WHERE id=?", (cur.lastrowid,)).fetchone()
    return _profile_dict(row)

def update_profile(pid: int, **fields: Any) -> Dict[str, Any]:
    if not fields: return get_profile(pid)
    for secret_field in ("api_token", "proxy_url"):
        if secret_field in fields:
            fields[secret_field] = _protect_db(fields[secret_field])
    fields["updated_at"] = iso(utc_now())
    sets = ", ".join(f"{k}=?" for k in fields)
    with db() as conn:
        previous = conn.execute("SELECT name FROM profiles WHERE id=?", (pid,)).fetchone()
        if not previous:
            raise HTTPException(404, f"Профиль {pid} не найден")
        next_name = fields.get("name")
        if next_name is not None and next_name != previous["name"]:
            conn.execute(
                "INSERT INTO profile_name_history(profile_id,old_name,new_name,changed_at) VALUES(?,?,?,?)",
                (pid, previous["name"], next_name, iso(utc_now())),
            )
        conn.execute(f"UPDATE profiles SET {sets} WHERE id=?", list(fields.values()) + [pid])
        row = conn.execute("SELECT * FROM profiles WHERE id=?", (pid,)).fetchone()
    return _profile_dict(row)

def delete_profile(pid: int) -> None:
    with db() as conn:
        conn.execute("DELETE FROM profiles WHERE id=?", (pid,))
        conn.execute("DELETE FROM run_logs WHERE profile_id=?", (pid,))
    with _state_lock:
        RUNNING_STATES.pop(pid, None)
        PROFILE_LOCKS.pop(pid, None)

def log_run(pid: int, *, started_at: datetime, finished_at: datetime,
            trigger_source: str, status: str, bumped_count: int = 0,
            items_found: int = 0, http_code: Optional[int] = None,
            item_ids: Optional[List[int]] = None, message: str = "") -> None:
    try:
        account_key = profile_account_key(get_profile(pid))
    except HTTPException:
        account_key = f"profile:{pid}"
    with db() as conn:
        conn.execute("""
            INSERT INTO run_logs
                (profile_id,started_at,finished_at,trigger_source,status,
                 bumped_count,items_found,http_code,account_key,item_ids,message)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """, (pid, iso(started_at), iso(finished_at), trigger_source, status,
              bumped_count, items_found, http_code, account_key,
              json.dumps(item_ids or []), message))


def get_direct_item_cooldowns(item_ids: List[int], now: Optional[datetime] = None) -> Dict[int, datetime]:
    """Return release times for ids successfully bumped during the last hour."""
    if not item_ids:
        return {}
    current = now or utc_now()
    since = iso(current - timedelta(minutes=DIRECT_ITEM_COOLDOWN_MINUTES))
    wanted = set(item_ids)
    releases: Dict[int, datetime] = {}
    with db() as conn:
        rows = conn.execute("""
            SELECT finished_at,item_ids FROM run_logs
            WHERE status IN ('ok','partial') AND bumped_count>0 AND finished_at>=?
            ORDER BY finished_at ASC, id ASC
        """, (since,)).fetchall()
    for row in rows:
        finished = parse_dt(row["finished_at"])
        if not finished:
            continue
        release = finished + timedelta(minutes=DIRECT_ITEM_COOLDOWN_MINUTES)
        if release <= current:
            continue
        try:
            logged_ids = json.loads(row["item_ids"] or "[]")
        except (TypeError, ValueError):
            continue
        for raw_id in logged_ids if isinstance(logged_ids, list) else []:
            try:
                item_id = int(raw_id)
            except (TypeError, ValueError):
                continue
            if item_id in wanted and (item_id not in releases or release > releases[item_id]):
                releases[item_id] = release
    return releases


# ── Pydantic ────────────────────────────────────────────────────────────────
class TokenCreate(BaseModel):
    name: str = ""; login: str = ""; api_token: str = ""; proxy_url: str = ""; color: str = ""
    proxy_id: Optional[int] = None
    daily_limit: int = Field(default=0, ge=0, le=100000)

class TokenUpdate(BaseModel):
    name: Optional[str] = None; login: Optional[str] = None
    api_token: Optional[str] = None; proxy_url: Optional[str] = None; color: Optional[str] = None
    proxy_id: Optional[int] = None
    daily_limit: Optional[int] = Field(default=None, ge=0, le=100000)

class ProxyPayload(BaseModel):
    name: str = ""
    scheme: str = "http"
    host: str = ""
    port: int = Field(default=8080, ge=1, le=65535)
    username: str = ""
    password: str = ""
    raw: str = ""

class ProxyBulkPayload(BaseModel):
    text: str = ""
    name_prefix: str = "Прокси"

class ProfileCreate(BaseModel):
    name:             str            = "Новая задача"
    token_id:         Optional[int]  = None
    api_token:        str            = ""
    proxy_id:         Optional[int]  = None
    proxy_url:        str            = ""
    target_mode:      Optional[str]  = None
    target_url:       str            = ""
    interval_locked:  bool           = False
    interval_minutes: int            = Field(default=60, ge=1, le=1440)
    bump_limit:       int            = Field(default=6, ge=1, le=100)

class ProfileUpdate(BaseModel):
    name:             Optional[str]  = None
    # Old/stale browser tabs used to submit the whole form and could overwrite a
    # task title with an account nickname.  A current client must explicitly
    # confirm that the title field itself was changed.
    name_confirmed:   bool           = False
    token_id:         Optional[int]  = None
    api_token:        Optional[str]  = None
    proxy_id:         Optional[int]  = None
    proxy_url:        Optional[str]  = None
    target_mode:      Optional[str]  = None
    target_url:       Optional[str]  = None
    interval_locked:  Optional[bool] = None
    interval_minutes: Optional[int]  = Field(default=None, ge=1, le=1440)
    bump_limit:       Optional[int]  = Field(default=None, ge=1, le=100)

class TogglePayload(BaseModel):
    enabled: bool

class SettingsUpdate(BaseModel):
    daily_limit:      Optional[int]  = None   # 0 = unlimited
    auto_distribute:  Optional[bool] = None
    distribution_mode: Optional[str] = None


# ── LZT Client ──────────────────────────────────────────────────────────────
class LZTClient:
    def __init__(self, token: str, proxy_url: str = "") -> None:
        if not token: raise RuntimeError("API токен не задан")
        self._token = str(token).strip()
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": f"{APP_NAME}/{API_VERSION}",
        }
        self.proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None

    @staticmethod
    def _request_error_status(exc: requests.RequestException) -> str:
        if isinstance(exc, (requests.exceptions.ProxyError, requests.exceptions.InvalidProxyURL)):
            return "proxy_error"
        if isinstance(exc, requests.exceptions.Timeout):
            return "timeout"
        if isinstance(exc, requests.exceptions.ConnectionError):
            return "connection_error"
        return "request_error"

    @classmethod
    def _request_error_message(cls, exc: requests.RequestException, attempts: int) -> str:
        status = cls._request_error_status(exc)
        suffix = f" после {attempts} попыток" if attempts > 1 else ""
        if status == "proxy_error":
            return (
                f"Прокси недоступен{suffix}. Проверь IP, порт, логин и пароль. "
                "Прямое соединение не использовалось."
            )
        if status == "timeout":
            return f"LZT Market не ответил вовремя{suffix}. Возможно, сайт временно недоступен."
        if status == "connection_error":
            return f"Нет соединения с LZT Market{suffix}. Проверь интернет или DNS."
        return f"Не удалось выполнить запрос к LZT Market{suffix}."

    @staticmethod
    def _payload_text(payload: Any, raw_text: str = "") -> str:
        values: List[str] = []
        if isinstance(payload, dict):
            for key in (
                "message", "error_description", "error", "detail", "errors",
                "_job_error", "_job_result",
            ):
                value = payload.get(key)
                if value not in (None, "", [], {}):
                    if isinstance(value, (dict, list)):
                        values.append(json.dumps(value, ensure_ascii=False))
                    else:
                        values.append(str(value))
        elif isinstance(payload, list) and payload:
            values.append(json.dumps(payload, ensure_ascii=False))
        elif payload not in (None, ""):
            values.append(str(payload))
        if not values and raw_text:
            values.append(raw_text)
        if not values and isinstance(payload, (dict, list)) and payload:
            values.append(json.dumps(payload, ensure_ascii=False))
        text = " · ".join(values)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:350]

    @classmethod
    def _http_error_result(cls, code: int, payload: Any, raw_text: str,
                           attempts: int) -> Dict[str, Any]:
        detail = cls._payload_text(payload, raw_text)
        tail = f" Ответ API: {detail}" if detail else ""
        if cls._is_bump_limit(detail):
            status, message = (
                "bump_limit",
                "Сработал лимит поднятия LZT: объявление уже поднималось слишком недавно.",
            )
        elif code == 401:
            status, message = "auth_error", "API-токен LZT недействителен или истёк. Проверь токен."
        elif code == 403:
            status, message = "access_denied", "LZT API запретил операцию. Проверь права токена и аккаунта."
        elif code == 429:
            status, message = (
                "rate_limited",
                f"Лимит частоты запросов LZT API не снялся после {attempts} попыток. Попробуй позже.",
            )
        elif code in RETRYABLE_HTTP_CODES or 500 <= code <= 599:
            status, message = (
                "maintenance",
                f"LZT Market или его API временно недоступен (HTTP {code}) после {attempts} попыток. "
                "Вероятно, идут технические работы.",
            )
        else:
            status, message = "api_error", f"LZT API отклонил запрос (HTTP {code})."
        return {
            "ok": False, "status": status, "http_code": code,
            "attempts": attempts, "message": message + tail,
        }

    @staticmethod
    def _retry_delay(attempt: int, response: Any = None) -> float:
        retry_after = None
        headers = getattr(response, "headers", None)
        if headers:
            try:
                retry_after = float(headers.get("Retry-After", ""))
            except (TypeError, ValueError):
                retry_after = None
        return max(0.25, min(10.0, retry_after if retry_after is not None else float(attempt)))

    @classmethod
    def _wait_for_retry(cls, attempt: int, response: Any = None) -> None:
        if attempt < MAX_REQUEST_ATTEMPTS:
            time.sleep(cls._retry_delay(attempt, response))

    @staticmethod
    def _extract_direct_ids(raw: str) -> List[int]:
        return extract_direct_ids(raw)

    def _search_urls(self, target_url: str) -> List[str]:
        def add_defaults(pairs):
            keys = {k for k, _ in pairs}
            if "show"     not in keys: pairs.append(("show", "active"))
            if "order_by" not in keys: pairs.append(("order_by", "pdate_to_up"))
            return pairs
        candidates: List[str] = []
        if target_url.startswith("http"):
            parsed = urlparse(target_url)
            pairs  = add_defaults(parse_qsl(parsed.query, keep_blank_values=True))
            query  = urlencode(pairs, doseq=True)
            if parsed.netloc == "api.lzt.market":
                candidates.append(f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{query}")
            else:
                candidates.append(f"https://api.lzt.market/user/items?{query}")
                m = re.search(r"/user/(\d+)/items", parsed.path)
                if m:
                    uid = m.group(1)
                    candidates.append(f"https://api.lzt.market/user/{uid}/items?{query}")
                    candidates.append(f"https://api.lzt.market/user/items?user_id={uid}&{query}")
        else:
            pairs = add_defaults(parse_qsl(target_url.lstrip("?"), keep_blank_values=True))
            candidates.append(f"https://api.lzt.market/user/items?{urlencode(pairs, doseq=True)}")
        seen: List[str] = []
        for u in candidates:
            if u not in seen: seen.append(u)
        return seen

    def fetch_items(self, target_url: str) -> Dict[str, Any]:
        target_url = target_url.strip()
        if not target_url:
            return {"ok": False, "status": "invalid_url", "http_code": None, "message": "URL не задан"}
        direct_ids = self._extract_direct_ids(target_url)
        if direct_ids:
            return {"ok": True, "status": "ok", "http_code": 200,
                    "items": [{"item_id": i} for i in direct_ids],
                    "attempts": 0, "message": f"Прямые аккаунты: {direct_ids}"}
        candidates = self._search_urls(target_url)
        if not candidates:
            return {"ok": False, "status": "invalid_url", "http_code": None,
                    "attempts": 0, "message": "Не удалось построить API-запрос"}

        attempts = 0
        candidate_index = 0
        last_error: Optional[Dict[str, Any]] = None
        last_exception: Optional[requests.RequestException] = None
        while attempts < MAX_REQUEST_ATTEMPTS and candidate_index < len(candidates):
            url = candidates[candidate_index]
            attempts += 1
            request_event = None
            try:
                request_event = wait_for_lzt_rate_limit(
                    "GET", url, self._token, source="Поднятие",
                )
                response = requests.get(
                    url, headers=self.headers, proxies=self.proxies, timeout=REQUEST_TIMEOUT,
                )
                observe_lzt_rate_limit("GET", url, self._token, response, request_event)
            except requests.RequestException as exc:
                fail_lzt_api_request(request_event, exc)
                last_exception = exc
                logger.warning("[Bump] GET, попытка %d/%d: %s",
                               attempts, MAX_REQUEST_ATTEMPTS, exc)
                self._wait_for_retry(attempts)
                continue
            last_exception = None

            try:
                data = response.json()
            except ValueError:
                data = None

            if response.status_code == 200 and isinstance(data, dict) and isinstance(data.get("items"), list):
                return {
                    "ok": True, "status": "ok", "http_code": 200,
                    "items": data.get("items") or [], "attempts": attempts,
                    "message": f"Список объявлений получен с попытки {attempts}",
                }

            if response.status_code == 200:
                last_error = {
                    "ok": False, "status": "api_error", "http_code": 200,
                    "attempts": attempts,
                    "message": "LZT API вернул неполный ответ без списка объявлений.",
                }
                logger.warning("[Bump] GET, попытка %d/%d: ответ API без items",
                               attempts, MAX_REQUEST_ATTEMPTS)
                self._wait_for_retry(attempts, response)
                continue

            last_error = self._http_error_result(
                response.status_code, data, getattr(response, "text", ""), attempts,
            )
            if response.status_code in (400, 404) and candidate_index + 1 < len(candidates):
                candidate_index += 1
                continue
            if last_error.get("status") != "bump_limit" and response.status_code in RETRYABLE_HTTP_CODES:
                self._wait_for_retry(attempts, response)
                continue
            return last_error

        if last_exception is not None and (last_error is None or attempts >= MAX_REQUEST_ATTEMPTS):
            status = self._request_error_status(last_exception)
            return {
                "ok": False, "status": status, "http_code": None, "attempts": attempts,
                "message": self._request_error_message(last_exception, attempts),
            }
        if last_error:
            last_error["attempts"] = attempts
            if last_error.get("http_code") in RETRYABLE_HTTP_CODES:
                return self._http_error_result(
                    int(last_error["http_code"]), None, "", attempts,
                )
            return last_error
        return {"ok": False, "status": "api_error", "http_code": None,
                "attempts": attempts, "message": "LZT API не вернул корректный ответ."}

    @staticmethod
    def _entry_code(entry: Dict[str, Any]) -> Optional[int]:
        objects = [entry]
        nested = entry.get("response")
        if isinstance(nested, dict):
            objects.append(nested)
        for obj in objects:
            # Batch jobs do not always include an HTTP code.  A successfully
            # executed LZT job is commonly confirmed through ``_job_result``.
            # Missing this confirmation used to schedule the same mutating
            # request again; the repeat then received the 60-second cooldown
            # even though the first bump had already succeeded.
            job_result = str(obj.get("_job_result", "")).strip().lower()
            if job_result in {"ok", "success", "successful", "completed", "complete", "done"}:
                return 200
            for key in ("httpCode", "http_code", "statusCode", "status_code", "code", "status"):
                value = obj.get(key)
                if isinstance(value, int):
                    return value
                if isinstance(value, str):
                    match = re.search(r"\b(\d{3})\b", value)
                    if match:
                        return int(match.group(1))
            if obj.get("success") is True or str(obj.get("status", "")).lower() in {"ok", "success", "completed"}:
                return 200
        return None

    @staticmethod
    def _entry_item_id(key: Any, entry: Dict[str, Any]) -> Optional[int]:
        for value in (entry.get("item_id"), entry.get("itemId"), entry.get("id"), key):
            match = re.search(r"(\d+)$", str(value or ""))
            if match:
                return int(match.group(1))
        return None

    @classmethod
    def _batch_entries(cls, payload: Any, pending: List[int]) -> Dict[int, Dict[str, Any]]:
        container = payload
        if isinstance(payload, dict):
            for key in ("jobs", "responses", "results", "data"):
                if isinstance(payload.get(key), (dict, list)):
                    container = payload[key]
                    break
        result: Dict[int, Dict[str, Any]] = {}
        if isinstance(container, dict):
            for key, entry in container.items():
                if not isinstance(entry, dict):
                    continue
                item_id = cls._entry_item_id(key, entry)
                if item_id in pending:
                    result[item_id] = entry
        elif isinstance(container, list):
            for index, entry in enumerate(container):
                if not isinstance(entry, dict):
                    continue
                item_id = cls._entry_item_id(None, entry)
                if item_id not in pending and index < len(pending):
                    item_id = pending[index]
                if item_id in pending:
                    result[item_id] = entry
        return result

    @staticmethod
    def _is_bump_limit(text: str) -> bool:
        value = text.lower()
        if any(marker in value for marker in (
            "bump limit", "limit of bump", "once per hour", "once an hour",
            "already bumped", "too soon", "hourly bump", "лимит подня",
            "не чаще", "раз в час", "60 минут", "час с последнего поднятия",
            "нужно подождать", "необходимо подождать", "подождите",
            "please wait", "wait before", "wait for",
        )):
            return True
        return bool(re.search(
            r"(?:подожд|ожида|wait).{0,48}\d+\s*(?:с(?:ек(?:унд[уы]?)?)?\.?|мин(?:ут[уы]?)?|sec(?:ond)?s?|min(?:ute)?s?)",
            value,
        ))

    @staticmethod
    def _is_item_unavailable(text: str, code: Optional[int] = None) -> bool:
        """Identify a per-item response that means the listing is no longer usable."""
        if code == 404:
            return True
        value = str(text or "").lower()
        return any(marker in value for marker in (
            "нет прав на просмотр этой страницы",
            "нет доступа к просмотру этой страницы",
            "у вас недостаточно прав для просмотра",
            "you do not have permission to view this page",
            "you don't have permission to view this page",
            "item not found",
            "listing not found",
            "объявление не найдено",
            "объявление удалено",
            "аккаунт уже продан",
        ))

    @classmethod
    def _item_error(cls, item_id: int, code: Optional[int], entry: Any) -> Dict[str, Any]:
        detail = cls._payload_text(entry)
        if cls._is_item_unavailable(detail, code):
            message = (
                f"#{item_id}: объявление больше недоступно для поднятия — "
                "вероятнее всего аккаунт уже продан."
            )
            status = "item_unavailable"
        elif cls._is_bump_limit(detail):
            message = f"#{item_id}: лимит поднятия LZT — объявление нельзя поднимать чаще разрешённого интервала."
            status = "bump_limit"
        elif is_maintenance_error(detail, code):
            message = f"#{item_id}: LZT Market находится на техническом обслуживании. Поднятие будет доступно после окончания работ."
            status = "maintenance"
        elif code == 401:
            message, status = f"#{item_id}: API-токен недействителен или истёк.", "auth_error"
        elif code == 403:
            message, status = f"#{item_id}: LZT API запретил поднятие для этого токена.", "access_denied"
        elif code == 429:
            message, status = f"#{item_id}: лимит частоты запросов LZT API.", "rate_limited"
        elif code in RETRYABLE_HTTP_CODES or (code is not None and 500 <= code <= 599):
            message, status = f"#{item_id}: LZT API временно недоступен (HTTP {code}).", "maintenance"
        elif code is not None:
            message, status = f"#{item_id}: LZT API вернул HTTP {code}.", "api_error"
        else:
            message, status = f"#{item_id}: LZT API не подтвердил результат поднятия.", "api_error"
        if detail:
            message += f" Ответ API: {detail}"
        return {"item_id": item_id, "status": status, "http_code": code, "message": message}

    @staticmethod
    def _batch_message(total: int, successful: List[int], errors: List[Dict[str, Any]]) -> str:
        if not errors:
            return f"Поднято: {len(successful)} из {total}."
        prefix = "Частично" if successful else "Не удалось выполнить поднятие"
        shown = "; ".join(error["message"] for error in errors[:5])
        if len(errors) > 5:
            shown += f"; ещё ошибок: {len(errors) - 5}"
        return f"{prefix}: поднято {len(successful)} из {total}. {shown}"

    def bump_items(self, item_ids: List[int]) -> Dict[str, Any]:
        requested: List[int] = []
        for raw_id in item_ids:
            try:
                item_id = int(raw_id)
            except (TypeError, ValueError):
                continue
            if item_id > 0 and item_id not in requested:
                requested.append(item_id)
        if not requested:
            return {"ok": True, "status": "ok", "http_code": 200, "attempts": 0,
                    "bumped_count": 0, "bumped_item_ids": [], "failed_item_ids": [],
                    "errors": [], "message": "Нет объявлений для поднятия."}

        # A single listing has an immediate confirmation response.  Multiple
        # listings use LZT's native bulk action (up to 5000 IDs) and are
        # confirmed when the API accepts the action into its queue.  The old
        # /batch jobs wrapper returned a different response shape and caused a
        # successful bump to be recorded as an error and then repeated.
        use_bulk = len(requested) > 1
        chunks = [requested] if not use_bulk else [requested[i:i + 5000] for i in range(0, len(requested), 5000)]
        successful: List[int] = []
        errors_by_id: Dict[int, Dict[str, Any]] = {}
        attempts = 0
        queued_count = 0
        last_http_code: Optional[int] = None

        for chunk in chunks:
            url = (
                "https://prod-api.lzt.market/items/bulk-action"
                if use_bulk else
                f"https://prod-api.lzt.market/{chunk[0]}/bump"
            )
            request_json = {"item_ids": chunk, "action": "bump"} if use_bulk else None
            chunk_finished = False
            last_error: Optional[Dict[str, Any]] = None

            for chunk_attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
                attempts += 1
                request_event = None
                try:
                    request_event = wait_for_lzt_rate_limit(
                        "POST",
                        url,
                        self._token,
                        batch_payload=request_json,
                        request_payload=request_json,
                        source="Поднятие",
                    )
                    response = requests.post(
                        url, json=request_json, headers=self.headers,
                        proxies=self.proxies, timeout=REQUEST_TIMEOUT,
                    )
                    observe_lzt_rate_limit("POST", url, self._token, response, request_event)
                except requests.RequestException as exc:
                    fail_lzt_api_request(request_event, exc)
                    status = self._request_error_status(exc)
                    last_error = {
                        "status": status, "http_code": None,
                        "message": self._request_error_message(exc, chunk_attempt),
                    }
                    logger.warning(
                        "[Bump] POST %s, попытка %d/%d: %s",
                        "bulk-action" if use_bulk else "item/bump",
                        chunk_attempt, MAX_REQUEST_ATTEMPTS, exc,
                    )
                    self._wait_for_retry(chunk_attempt)
                    continue

                last_http_code = response.status_code
                try:
                    payload = response.json()
                except ValueError:
                    payload = None

                if 200 <= response.status_code < 300:
                    if use_bulk:
                        action = str(payload.get("action", "")).strip().lower() if isinstance(payload, dict) else ""
                        queued = isinstance(payload, dict) and payload.get("queued") is True
                        items_count = payload.get("items_count") if isinstance(payload, dict) else None
                        try:
                            accepted_count = int(items_count) if items_count is not None else len(chunk)
                        except (TypeError, ValueError):
                            accepted_count = -1
                        count_matches = accepted_count == len(chunk)
                        confirmed = queued and action == "bump" and count_matches
                    else:
                        status_value = str(payload.get("status", "")).strip().lower() if isinstance(payload, dict) else ""
                        message_value = str(payload.get("message", "")).strip().lower() if isinstance(payload, dict) else ""
                        confirmed = (
                            status_value in {"ok", "success", "successful", "completed"}
                            or (isinstance(payload, dict) and payload.get("success") is True)
                            or "был поднят" in message_value
                        )

                    if confirmed:
                        last_error = None
                        successful.extend(chunk)
                        if use_bulk:
                            queued_count += len(chunk)
                        chunk_finished = True
                        logger.info(
                            "[Bump] %s подтверждено: %d объявлений",
                            "bulk-action принят в очередь" if use_bulk else "прямое поднятие",
                            len(chunk),
                        )
                        break

                    # Do not blindly repeat an already accepted mutating HTTP
                    # request just because its successful 2xx payload changed.
                    detail = self._payload_text(payload, getattr(response, "text", ""))
                    message = "LZT API вернул успешный HTTP-ответ, но формат подтверждения поднятия неизвестен."
                    if detail:
                        message += f" Ответ API: {detail}"
                    last_error = {
                        "status": "unconfirmed", "http_code": response.status_code,
                        "message": message,
                    }
                    chunk_finished = True
                    break

                last_error = self._http_error_result(
                    response.status_code, payload, getattr(response, "text", ""), chunk_attempt,
                )
                if last_error.get("status") != "bump_limit" and response.status_code in RETRYABLE_HTTP_CODES:
                    self._wait_for_retry(chunk_attempt, response)
                    continue
                chunk_finished = True
                break

            if not chunk_finished and last_error is None:
                last_error = {
                    "status": "api_error", "http_code": last_http_code,
                    "message": "LZT API не подтвердил результат поднятия после повторов.",
                }

            if not chunk_finished or last_error is not None:
                error_status = str((last_error or {}).get("status") or "api_error")
                error_code = (last_error or {}).get("http_code")
                error_message = str((last_error or {}).get("message") or "LZT API не подтвердил результат поднятия.")
                for item_id in chunk:
                    # Preserve item-specific classifications such as a sold
                    # listing or the one-hour bump limit whenever API details
                    # are available in the error message.
                    item_error = self._item_error(item_id, error_code, {"error": error_message})
                    if item_error.get("status") == "api_error" and error_status != "api_error":
                        item_error["status"] = error_status
                    errors_by_id[item_id] = item_error

        successful = [item_id for item_id in requested if item_id in set(successful)]
        errors = [errors_by_id[item_id] for item_id in requested if item_id in errors_by_id]
        if successful and errors:
            status, ok = "partial", True
        elif successful:
            status, ok = "ok", True
        else:
            statuses = {error["status"] for error in errors}
            status = "bump_limit" if "bump_limit" in statuses else "api_error"
            if len(statuses) == 1:
                status = next(iter(statuses))
            ok = False
        return {
            "ok": ok, "status": status, "http_code": last_http_code,
            "attempts": attempts, "bumped_count": len(successful),
            "queued_count": queued_count,
            "bumped_item_ids": successful,
            "failed_item_ids": [error["item_id"] for error in errors],
            "errors": errors,
            "message": (
                f"Запрос принят LZT: в очередь на поднятие передано {queued_count} из {len(requested)}."
                if queued_count and not errors else
                self._batch_message(len(requested), successful, errors)
            ),
        }


# ── Bump Service ─────────────────────────────────────────────────────────────
class BumpService:
    def __init__(self):
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    def start(self):
        if self._thread and self._thread.is_alive(): return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="bump-scheduler")
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread and self._thread.is_alive(): self._thread.join(timeout=5)

    def _loop(self):
        while not self._stop.wait(CHECK_EVERY_SECONDS):
            for p in get_all_profiles():
                try:
                    if not p["enabled"]: continue
                    pid = p["id"]
                    if _get_rs(pid)["is_running"]: continue
                    nxt = parse_dt(p["next_run_at"])
                    if nxt is None:
                        update_profile(pid, next_run_at=iso(utc_now() + timedelta(minutes=p["interval_minutes"])))
                        continue
                    if utc_now() >= nxt:
                        threading.Thread(target=self.run_once, args=(pid, "scheduled"),
                                         daemon=True, name=f"bump-p{pid}").start()
                except HTTPException as exc:
                    # The task may have been deleted after the scheduler read its
                    # snapshot.  That is a normal race and must not kill the loop.
                    if exc.status_code != 404:
                        logger.warning("[Scheduler] P%d: %s", p.get("id", 0), exc.detail)
                except Exception as exc:
                    logger.exception("[Scheduler] Ошибка задачи P%d: %s", p.get("id", 0), exc)

    def run_once(self, pid: int, trigger_source: str = "manual") -> Dict[str, Any]:
        lock = _get_lock(pid)
        if not lock.acquire(blocking=False):
            return {"ok": False, "status": "busy", "message": "Цикл уже выполняется"}
        if not _budget_lock.acquire(blocking=False):
            lock.release()
            return {"ok": False, "status": "busy", "message": "Другая задача сейчас выполняет поднятие"}

        started_at = utc_now()
        _set_rs(pid, is_running=True, started_at=iso(started_at), trigger=trigger_source)
        trigger_ru = "планировщик" if trigger_source == "scheduled" else "ручной"
        logger.info("[P%d] ┌─ Начало [%s]", pid, trigger_ru)

        try:
            p = get_profile(pid)
            if p.get("proxy_missing"):
                return self._fail(
                    pid, started_at, trigger_source, p,
                    "Запуск заблокирован: назначенный прокси удалён. Выбери новый прокси в задаче.",
                )
            effective_bump_limit = p["bump_limit"]
            account_key = profile_account_key(p)
            daily_limit = profile_daily_limit(p)
            # The account limit is a safety boundary and is always enforced.
            # The auto-mode switch controls schedule recalculation only.
            if daily_limit > 0:
                rolling = get_account_budget_status(account_key, daily_limit)
                if rolling["limit_reached"]:
                    release_at = parse_dt(rolling["next_release_at"])
                    next_run = (release_at + timedelta(seconds=2)) if release_at else (utc_now() + timedelta(minutes=1))
                    message = (
                        f"Лимит {daily_limit} за 24 часа достигнут. "
                        f"Освободится {rolling['next_release_count']} поднятий после окончания окна."
                    )
                    update_profile(pid, next_run_at=iso(next_run), last_message=message)
                    logger.info("[Smart] [%s] [P%d] лимит достигнут, ожидание до %s",
                                account_key, pid, next_run.strftime("%H:%M:%S"))
                    return {
                        "ok": False,
                        "status": "limit_wait",
                        "message": message,
                        "next_run_at": iso(next_run),
                        **rolling,
                    }
                available = (
                    rolling["remaining"] if p["interval_locked"]
                    else rolling["auto_remaining"]
                )
                if available is not None and int(available) <= 0:
                    next_run = utc_now() + timedelta(minutes=p["interval_minutes"])
                    message = (
                        f"Автоматический пул аккаунта использован. "
                        f"Ещё {rolling['reserved_remaining']} поднятий сохранено "
                        "для зафиксированных задач."
                    )
                    update_profile(pid, next_run_at=iso(next_run), last_message=message)
                    logger.info("[Smart] [%s] [P%d] сохранён резерв %d, ожидание до %s",
                                account_key, pid, rolling["reserved_remaining"],
                                next_run.strftime("%H:%M:%S"))
                    return {
                        "ok": False, "status": "limit_wait", "message": message,
                        "next_run_at": iso(next_run), **rolling,
                    }
                if available is not None:
                    effective_bump_limit = min(effective_bump_limit, int(available))

            api_token = p["api_token"]
            if p.get("token_id"):
                tok = get_token(int(p["token_id"]))
                if tok and tok["api_token"]: api_token = tok["api_token"]

            try: client = LZTClient(token=api_token, proxy_url=p["proxy_url"])
            except RuntimeError as exc:
                return self._fail(pid, started_at, trigger_source, p, str(exc))

            fetch = client.fetch_items(p["target_url"])
            if not fetch["ok"]:
                return self._fail(pid, started_at, trigger_source, p, fetch["message"],
                                  http_code=fetch.get("http_code"),
                                  result_status=fetch.get("status", "error"))

            items = fetch.get("items") or []
            candidate_ids: List[int] = []
            for item in items:
                iid = item.get("item_id") if isinstance(item, dict) else None
                if isinstance(iid, int): item_id = iid
                elif isinstance(iid, str) and iid.isdigit(): item_id = int(iid)
                else: continue
                if item_id not in candidate_ids:
                    candidate_ids.append(item_id)

            direct_ids = extract_direct_ids(p.get("target_url", ""))
            cooldowns: Dict[int, datetime] = {}
            cooldown_skipped = 0
            if direct_ids:
                cooldowns = get_direct_item_cooldowns(candidate_ids)
                eligible_ids = [item_id for item_id in candidate_ids if item_id not in cooldowns]
                cooldown_skipped = len(candidate_ids) - len(eligible_ids)
                ids = eligible_ids[:effective_bump_limit]
            else:
                ids = candidate_ids[:effective_bump_limit]

            logger.info("[P%d] │  Найдено: %d, бампим: %d", pid, len(items), len(ids))

            if not ids:
                finished_at = utc_now()
                if direct_ids and cooldowns:
                    release_at = min(cooldowns.values())
                    wait_until = release_at + timedelta(seconds=2)
                    wait_seconds = max(1, math.ceil((release_at - finished_at).total_seconds()))
                    wait_minutes = max(1, math.ceil(wait_seconds / 60))
                    msg = (
                        f"Все {len(candidate_ids)} аккаунтов выдерживают паузу 60 минут. "
                        f"Ближайший освободится примерно через {wait_minutes} мин."
                    )
                    update_profile(
                        pid, last_run_at=iso(finished_at), next_run_at=iso(wait_until),
                        last_status="waiting", last_message=msg,
                    )
                    log_run(
                        pid, started_at=started_at, finished_at=finished_at,
                        trigger_source=trigger_source, status="cooldown",
                        items_found=len(items), message=msg,
                    )
                    logger.info("[P%d] │  ⏳ Все прямые аккаунты на паузе до %s",
                                pid, wait_until.strftime("%H:%M:%S"))
                    return {
                        "ok": True, "status": "cooldown", "message": msg,
                        "items_found": len(items), "bumped_count": 0,
                        "cooldown_skipped": cooldown_skipped,
                        "next_run_at": iso(wait_until),
                    }
                msg = "Нет активных объявлений"
                update_profile(pid, last_run_at=iso(finished_at),
                               next_run_at=iso(finished_at + timedelta(minutes=p["interval_minutes"])),
                               last_status="ok", last_message=msg)
                log_run(pid, started_at=started_at, finished_at=finished_at,
                        trigger_source=trigger_source, status="ok", items_found=len(items), message=msg)
                self._maybe_redistribute()
                return {"ok": True, "status": "ok", "message": msg,
                        "items_found": len(items), "bumped_count": 0}

            bump = client.bump_items(ids)
            finished_at = utc_now()
            msg    = bump.get("message", "")
            cnt    = int(bump.get("bumped_count", 0))
            bumped_ids = bump.get("bumped_item_ids") or ([] if cnt == 0 else ids[:cnt])
            unavailable_ids = [
                int(error["item_id"])
                for error in (bump.get("errors") or [])
                if error.get("status") == "item_unavailable" and error.get("item_id") is not None
            ]
            removed_ids: List[int] = []
            task_disabled = False
            if profile_target_mode(p) == "direct" and unavailable_ids:
                removed_ids, task_disabled = prune_unavailable_direct_items(pid, unavailable_ids)

            failed_ids = [int(item_id) for item_id in (bump.get("failed_item_ids") or [])]
            handled_unavailable = bool(
                removed_ids and failed_ids and set(failed_ids) == set(removed_ids)
            )
            result_ok = bool(bump["ok"] or (cnt == 0 and handled_unavailable))
            if cnt == 0 and handled_unavailable:
                status = "cleaned"
                labels = ", ".join(f"#{item_id}" for item_id in removed_ids)
                suffix = " Последний точечный аккаунт удалён — задача выключена." if task_disabled else ""
                msg = (
                    f"{labels}: вероятнее всего аккаунт уже продан или больше недоступен. "
                    f"Удалено из точечного списка.{suffix}"
                )
            else:
                status = "partial" if bump.get("status") == "partial" else ("ok" if bump["ok"] else "error")
                if removed_ids:
                    labels = ", ".join(f"#{item_id}" for item_id in removed_ids)
                    msg = (
                        f"{msg} {labels}: вероятнее всего уже продан или недоступен; "
                        "удалено из точечного списка."
                    )
                    if cnt > 0:
                        status = "partial"
            if bump["ok"] and cooldown_skipped:
                msg = (
                    f"{msg}. Пропущено на 60-минутной паузе: {cooldown_skipped}; "
                    f"выбраны следующие доступные аккаунты."
                )

            update_profile(pid, last_run_at=iso(finished_at),
                           next_run_at=(None if task_disabled else iso(finished_at + timedelta(minutes=p["interval_minutes"]))),
                           last_status=status, last_message=msg)
            history_item_ids = list(dict.fromkeys([*bumped_ids, *removed_ids]))
            log_run(pid, started_at=started_at, finished_at=finished_at,
                    trigger_source=trigger_source, status=status,
                    bumped_count=cnt, items_found=len(items),
                    http_code=bump.get("http_code"), item_ids=history_item_ids, message=msg)

            if status == "cleaned":
                logger.info("[P%d] │  Удалены недоступные точечные аккаунты: %s", pid, removed_ids)
            elif bump["ok"]:
                logger.info("[P%d] │  ✓ Поднято: %d", pid, cnt)
            else:
                logger.error("[P%d] │  ✗ %s", pid, msg)
            if task_disabled:
                logger.info("[P%d] └─ Точечный список пуст — задача выключена", pid)
            else:
                logger.info("[P%d] └─ Следующий в %s", pid,
                            (finished_at + timedelta(minutes=p["interval_minutes"])).strftime("%H:%M:%S"))

            self._maybe_redistribute()

            return {"ok": result_ok, "status": status, "message": msg,
                    "items_found": len(items), "bumped_count": cnt,
                    "bumped_item_ids": bumped_ids,
                    "failed_item_ids": bump.get("failed_item_ids", []),
                    "removed_item_ids": removed_ids,
                    "attempts": bump.get("attempts", 0),
                    "cooldown_skipped": cooldown_skipped}

        except HTTPException as exc:
            if exc.status_code == 404:
                logger.info("[P%d] Задача удалена во время выполнения", pid)
                return {"ok": False, "status": "deleted", "message": "Задача удалена"}
            raise
        except Exception as exc:
            finished_at = utc_now()
            logger.exception("[P%d] └─ Ошибка: %s", pid, exc)
            try:
                p2 = get_profile(pid)
                update_profile(pid, last_run_at=iso(finished_at),
                               next_run_at=iso(finished_at + timedelta(minutes=p2["interval_minutes"])),
                               last_status="error", last_message=str(exc))
                log_run(pid, started_at=started_at, finished_at=finished_at,
                        trigger_source=trigger_source, status="error", message=str(exc))
            except HTTPException as missing:
                if missing.status_code != 404:
                    raise
            return {"ok": False, "status": "error", "message": str(exc)}
        finally:
            _set_rs(pid, is_running=False, started_at=None, trigger=None)
            _budget_lock.release()
            lock.release()

    def _fail(self, pid, started_at, trigger_source, p, msg, http_code=None,
              result_status="error"):
        finished_at = utc_now()
        update_profile(pid, last_run_at=iso(finished_at),
                       next_run_at=iso(finished_at + timedelta(minutes=p["interval_minutes"])),
                       last_status="error", last_message=msg)
        log_run(pid, started_at=started_at, finished_at=finished_at,
                trigger_source=trigger_source, status="error",
                http_code=http_code, message=msg)
        return {"ok": False, "status": result_status, "message": msg,
                "http_code": http_code}

    def _maybe_redistribute(self):
        try:
            if get_setting("auto_distribute", "0") != "1": return
            groups: Dict[str, Dict[str, Any]] = {}
            for profile in get_all_profiles():
                if not profile["enabled"]:
                    continue
                key = profile_account_key(profile)
                groups.setdefault(key, {
                    "limit": profile_daily_limit(profile), "profiles": [],
                })["profiles"].append(profile)
            applied_mode = get_setting(
                "distribution_applied_mode", get_setting("distribution_mode", "batch")
            )
            for account_key, group in groups.items():
                limit = int(group["limit"])
                if limit <= 0:
                    continue
                rolling = get_rolling_limit_status(limit, account_key)
                if rolling["limit_reached"]:
                    release_at = parse_dt(rolling["next_release_at"])
                    wait_until = (
                        release_at + timedelta(seconds=2)
                        if release_at else utc_now() + timedelta(minutes=1)
                    )
                    for profile in group["profiles"]:
                        current_next = parse_dt(profile["next_run_at"])
                        target = max(wait_until, current_next) if current_next else wait_until
                        update_profile(profile["id"], next_run_at=iso(target))
                        logger.info(
                            "[Smart] [%s] лимит %d достигнут (%d), P%d ждёт до %s",
                            account_key, limit, rolling["today_bumps"], profile["id"],
                            target.strftime("%H:%M:%S"),
                        )
                else:
                    result = auto_distribute_intervals(
                        limit, applied_mode, apply_changes=True, account_key=account_key,
                    )
                    plans = {
                        int(update["id"]): update
                        for update in result.get("updates", [])
                        if update.get("changed")
                    }
                    now = utc_now()
                    for profile in group["profiles"]:
                        plan = plans.get(int(profile["id"]))
                        if not plan or profile["interval_locked"]:
                            continue
                        last_run = parse_dt(profile.get("last_run_at"))
                        next_run = (
                            last_run + timedelta(minutes=int(plan["interval_minutes"]))
                            if last_run else now + timedelta(minutes=int(plan["interval_minutes"]))
                        )
                        if next_run <= now:
                            next_run = now + timedelta(seconds=2)
                        update_profile(profile["id"], next_run_at=iso(next_run))
                        logger.info(
                            "[Smart] [%s] [P%d] следующий запуск скорректирован на %s",
                            account_key, profile["id"], next_run.strftime("%H:%M:%S"),
                        )
        except Exception as exc:
            logger.warning("[Smart] Ошибка авто-распределения: %s", exc)


# ── FastAPI ──────────────────────────────────────────────────────────────────
service = BumpService()
app     = FastAPI(title=APP_NAME)
utilities_router, utilities_service = create_utilities_router(
    arb.load_arb_config,
    BASE_DIR / "utilities" / "data" / "resale_finder_state.json",
    get_token,
)

class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Any) -> Response:
        started = time.perf_counter()
        try:
            res = await call_next(request)
        except Exception:
            elapsed = (time.perf_counter() - started) * 1000
            logger.exception(
                "[HTTP] %s %s → необработанная ошибка за %.0f мс",
                request.method, request.url.path, elapsed,
            )
            raise
        elapsed = (time.perf_counter() - started) * 1000
        if res.status_code >= 400:
            # A stale page can briefly address a profile that has just been
            # deleted.  It is handled by the UI and is not a server failure.
            if res.status_code == 404 and request.url.path.startswith("/api/profiles/"):
                # Old browser tabs can keep retrying an autosave for a task that
                # no longer exists. The response is intentional; do not flood
                # the application console with harmless tombstone requests.
                pass
            else:
                reasons = {
                    400: "некорректные данные", 401: "требуется авторизация",
                    403: "доступ запрещён", 404: "ресурс не найден",
                    409: "конфликт состояния", 422: "ошибка проверки данных",
                    429: "слишком много запросов", 500: "внутренняя ошибка",
                    502: "ошибка внешнего сервиса", 503: "сервис временно недоступен",
                    504: "внешний сервис не ответил вовремя",
                }
                log = logger.error if res.status_code >= 500 else logger.warning
                log(
                    "[HTTP] %s %s → %d (%s), %.0f мс",
                    request.method, request.url.path, res.status_code,
                    reasons.get(res.status_code, "запрос отклонён"), elapsed,
                )
        elif request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            logger.info(
                "[HTTP] %s %s → %d, %.0f мс",
                request.method, request.url.path, res.status_code, elapsed,
            )
        res.headers.setdefault("X-Content-Type-Options", "nosniff")
        res.headers.setdefault("X-Frame-Options", "DENY")
        res.headers.setdefault("Referrer-Policy", "no-referrer")
        res.headers.setdefault(
            "Permissions-Policy",
            "camera=(), microphone=(), geolocation=(), payment=(), usb=()",
        )
        res.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'self'; connect-src 'self'; img-src 'self' data:; "
            "style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; "
            "font-src 'self'; object-src 'none'; frame-ancestors 'none'; "
            "base-uri 'none'; form-action 'self'",
        )
        if request.url.path.startswith("/api/"):
            res.headers.setdefault("Cache-Control", "no-store")
        return res

app.add_middleware(LoggingMiddleware)
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["127.0.0.1", "localhost", "[::1]", "testserver"],
)

STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
if TUTORIAL_DIR.is_dir():
    app.mount("/tutorial", StaticFiles(directory=str(TUTORIAL_DIR)), name="tutorial")
app.include_router(arb.router)
app.include_router(utilities_router)


@app.on_event("startup")
def on_startup():
    logging.getLogger("uvicorn.access").disabled = True
    init_db()
    # Rebuild the schedule from persisted rolling history before overdue tasks
    # can run after an application restart.
    service._maybe_redistribute()
    service.start()
    arb.autostart()
    auto  = get_setting("auto_distribute", "0") == "1"
    account_limits = ", ".join(
        f"{token.get('name') or token['id']}={int(token.get('daily_limit', 0)) or '—'}"
        for token in get_all_tokens()
    ) or "—"
    logger.info("[SYSTEM] %s запущен", APP_NAME)
    logger.info("[SYSTEM] Панель: http://127.0.0.1:8787")
    logger.info("[SYSTEM] Задач: %d | токенов: %d | лимиты/24ч: %s | авто: %s",
                len(get_all_profiles()), len(get_all_tokens()),
                account_limits, "вкл" if auto else "выкл")
    logger.info("[SYSTEM] Проверка: %s", "работает" if arb.service.running else
                ("выключен" if arb.ARB_AVAILABLE else "недоступен"))

@app.on_event("shutdown")
def on_shutdown():
    service.stop()
    arb.service.stop()
    utilities_service.stop()


@app.get("/", response_class=HTMLResponse)
def serve_panel():
    index = STATIC_DIR / "index.html"
    return (FileResponse(str(index), headers={"Cache-Control": "no-store"})
            if index.exists() else HTMLResponse("<h1>index.html не найден</h1>"))

@app.get("/health")
def health():
    return {"ok": True, "api_version": API_VERSION, "tasks": len(get_all_profiles()), "tokens": len(get_all_tokens()),
            "proxies": len(get_all_proxies()),
            "today_bumps": get_today_bumps()}


def _api_monitor_labels() -> Dict[str, Dict[str, str]]:
    """Map irreversible token fingerprints to local display names."""
    labels: Dict[str, Dict[str, str]] = {}
    for token in get_all_tokens():
        secret = str(token.get("api_token") or "").strip()
        if not secret:
            continue
        labels[lzt_token_fingerprint(secret)] = {
            "name": str(token.get("name") or f"Аккаунт {token.get('id') or ''}").strip(),
            "color": str(token.get("color") or "#10b981"),
            "source": "Менеджер аккаунтов",
        }

    # Legacy tasks may still contain a token directly instead of a token_id.
    for profile in get_all_profiles():
        secret = str(profile.get("api_token") or "").strip()
        if not secret:
            continue
        labels.setdefault(lzt_token_fingerprint(secret), {
            "name": str(profile.get("owner_name") or profile.get("name") or "Ручной токен"),
            "color": str(profile.get("owner_color") or "#6366f1"),
            "source": "Задача поднятия",
        })

    if arb.ARB_AVAILABLE:
        try:
            cfg = arb.service.config if arb.service.running else arb.load_arb_config()
            secret = str(cfg.get("token") or "").strip()
            if secret:
                labels.setdefault(lzt_token_fingerprint(secret), {
                    "name": str(cfg.get("nickname") or "Проверка"),
                    "color": "#10b981",
                    "source": "Настройки проверки",
                })
        except Exception:
            logger.debug("[API Monitor] Не удалось прочитать токен проверки", exc_info=True)
    return labels


@app.get("/api/api-monitor")
def api_api_monitor():
    return {"ok": True, **get_lzt_api_monitor(_api_monitor_labels())}


# ── Settings API ─────────────────────────────────────────────────────────────
def _limit_accounts_payload() -> List[Dict[str, Any]]:
    profiles = get_all_profiles()
    result: List[Dict[str, Any]] = []
    known_keys: set[str] = set()
    for token in get_all_tokens():
        account_key = f"token:{int(token['id'])}"
        known_keys.add(account_key)
        owned = [p for p in profiles if profile_account_key(p) == account_key]
        enabled = [p for p in owned if p["enabled"]]
        limit = max(0, int(token.get("daily_limit", 0)))
        locked_bpd = sum(estimated_profile_bpd(p) for p in enabled if p["interval_locked"])
        budget = get_account_budget_status(account_key, limit)
        result.append({
            "token_id": int(token["id"]), "account_key": account_key,
            "name": token.get("name") or f"Аккаунт {token['id']}",
            "color": token.get("color") or "", "daily_limit": limit,
            "tasks": len(owned), "enabled_tasks": len(enabled),
            "locked_bpd": round(locked_bpd, 1),
            **budget,
        })

    # Keep legacy inline-token tasks usable, but encourage moving them into the
    # account manager where their independent limit can be edited explicitly.
    manual_groups: Dict[str, List[Dict[str, Any]]] = {}
    for profile in profiles:
        key = profile_account_key(profile)
        if key not in known_keys:
            manual_groups.setdefault(key, []).append(profile)
    for index, (account_key, owned) in enumerate(manual_groups.items(), 1):
        enabled = [p for p in owned if p["enabled"]]
        limit = profile_daily_limit(owned[0]) if owned else 0
        locked_bpd = sum(estimated_profile_bpd(p) for p in enabled if p["interval_locked"])
        budget = get_account_budget_status(account_key, limit)
        result.append({
            "token_id": None, "account_key": account_key,
            "name": "Ручной токен" if len(manual_groups) == 1 else f"Ручной токен {index}",
            "color": "", "daily_limit": limit, "legacy": True,
            "tasks": len(owned), "enabled_tasks": len(enabled),
            "locked_bpd": round(locked_bpd, 1),
            **budget,
        })
    return result


def _settings_payload() -> Dict[str, Any]:
    s = get_all_settings()
    return {
        "ok": True,
        "api_version": API_VERSION,
        "daily_limit": int(s.get("daily_limit", "0")),  # legacy inline-token fallback
        "auto_distribute": s.get("auto_distribute", "0") == "1",
        "distribution_mode": s.get("distribution_mode", "batch"),
        "distribution_applied_mode": s.get(
            "distribution_applied_mode", s.get("distribution_mode", "batch")
        ),
        "distribution_modes": [
            {"id": key, **value} for key, value in DISTRIBUTION_MODES.items()
        ],
        "account_limits": _limit_accounts_payload(),
        "today_bumps": get_today_bumps(),
    }


@app.get("/api/settings")
def api_get_settings():
    return _settings_payload()

@app.put("/api/settings")
def api_put_settings(payload: SettingsUpdate):
    if payload.daily_limit is not None:
        set_setting("daily_limit", str(max(0, payload.daily_limit)))
    if payload.auto_distribute is not None:
        set_setting("auto_distribute", "1" if payload.auto_distribute else "0")
    if payload.distribution_mode is not None:
        if payload.distribution_mode not in DISTRIBUTION_MODES:
            raise HTTPException(400, "Неизвестный режим распределения")
        if not get_setting("distribution_applied_mode", ""):
            set_setting("distribution_applied_mode", get_setting("distribution_mode", "batch"))
        set_setting("distribution_mode", payload.distribution_mode)
    if payload.auto_distribute is True:
        service._maybe_redistribute()
    return _settings_payload()


def _distribution_token_context(token_id: int) -> tuple[Dict[str, Any], str, int]:
    token = get_token(token_id)
    if not token:
        raise HTTPException(404, "Аккаунт не найден")
    account_key = f"token:{token_id}"
    return token, account_key, max(0, int(token.get("daily_limit", 0)))


@app.post("/api/auto-distribute/preview")
def api_preview_auto_distribute(token_id: Optional[int] = None):
    mode = get_setting("distribution_mode", "batch")
    if token_id is None:
        return auto_distribute_all_accounts(mode, apply_changes=False)
    token, account_key, limit = _distribution_token_context(token_id)
    if limit <= 0:
        raise HTTPException(400, "Установи лимит поднятий для выбранного аккаунта")
    result = auto_distribute_intervals(
        limit, mode, apply_changes=False, account_key=account_key,
    )
    result.update(get_rolling_limit_status(limit, account_key))
    result.update(token_id=token_id, account_name=token.get("name") or f"Аккаунт {token_id}")
    return result

@app.post("/api/auto-distribute")
def api_auto_distribute(token_id: Optional[int] = None):
    mode = get_setting("distribution_mode", "batch")
    if token_id is None:
        result = auto_distribute_all_accounts(mode, apply_changes=True)
        if result.get("accounts"):
            set_setting("distribution_applied_mode", mode)
        for account in result.get("accounts", []):
            account_key = str(account["account_key"])
            limit = int(account["daily_limit"])
            rolling = get_rolling_limit_status(limit, account_key)
            release_at = parse_dt(rolling["next_release_at"])
            for profile in get_all_profiles():
                if not profile["enabled"] or profile_account_key(profile) != account_key:
                    continue
                next_run = utc_now() + timedelta(minutes=profile["interval_minutes"])
                if rolling["limit_reached"] and release_at:
                    next_run = max(next_run, release_at + timedelta(seconds=2))
                update_profile(profile["id"], next_run_at=iso(next_run))
        return result
    token, account_key, limit = _distribution_token_context(token_id)
    if limit <= 0:
        raise HTTPException(400, "Установи лимит поднятий для выбранного аккаунта")
    result = auto_distribute_intervals(
        limit, mode, apply_changes=True, account_key=account_key,
    )
    if result.get("ok"):
        set_setting("distribution_applied_mode", mode)
    if result.get("updates"):
        rolling = get_rolling_limit_status(limit, account_key)
        release_at = parse_dt(rolling["next_release_at"])
        for p in get_all_profiles():
            if p["enabled"] and profile_account_key(p) == account_key and p["next_run_at"]:
                next_run = utc_now() + timedelta(minutes=p["interval_minutes"])
                if rolling["limit_reached"] and release_at:
                    next_run = max(next_run, release_at + timedelta(seconds=2))
                update_profile(p["id"], next_run_at=iso(next_run))
    result.update(get_rolling_limit_status(limit, account_key))
    result.update(token_id=token_id, account_name=token.get("name") or f"Аккаунт {token_id}")
    return result

@app.get("/api/stats/today")
def api_today_stats():
    return {"ok": True, "today_bumps": get_today_bumps(),
            "account_limits": _limit_accounts_payload(),
            "per_profile": get_today_bumps_per_profile()}


# ── Proxies API ───────────────────────────────────────────────────────────────
def _proxy_safe(proxy: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": proxy["id"],
        "name": proxy["name"],
        "scheme": proxy["scheme"],
        "host": proxy["host"],
        "port": proxy["port"],
        "username": proxy["username"],
        "has_password": bool(proxy["password"]),
        "address": f"{proxy['host']}:{proxy['port']}",
        "profile_count": int(proxy.get("profile_count", 0)),
        "token_count": int(proxy.get("token_count", 0)),
    }


@app.get("/api/proxies")
def api_list_proxies():
    return {"ok": True, "proxies": [_proxy_safe(p) for p in get_all_proxies()]}


@app.get("/api/proxies/{proxy_id}")
def api_get_proxy(proxy_id: int):
    proxy = get_proxy(proxy_id)
    if not proxy:
        raise HTTPException(404, "Прокси не найден")
    return {"ok": True, "proxy": proxy}


@app.post("/api/proxies")
def api_create_proxy(payload: ProxyPayload):
    try:
        proxy = create_proxy(
            payload.name, payload.raw.strip(), scheme=payload.scheme, host=payload.host.strip(),
            port=payload.port, username=payload.username.strip(), password=payload.password,
        )
    except (ValueError, TypeError) as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"ok": True, "proxy": _proxy_safe(proxy)}


@app.put("/api/proxies/{proxy_id}")
def api_update_proxy(proxy_id: int, payload: ProxyPayload):
    try:
        proxy = update_proxy(
            proxy_id, payload.name, payload.raw.strip(), scheme=payload.scheme,
            host=payload.host.strip(), port=payload.port,
            username=payload.username.strip(), password=payload.password,
        )
    except (ValueError, TypeError) as exc:
        raise HTTPException(400, str(exc)) from exc
    if not proxy:
        raise HTTPException(404, "Прокси не найден")
    return {"ok": True, "proxy": _proxy_safe(proxy)}


@app.post("/api/proxy-import")
def api_import_proxies(payload: ProxyBulkPayload):
    lines = [line.strip() for line in re.split(r"[\r\n]+", payload.text) if line.strip()]
    if not lines:
        raise HTTPException(400, "Добавь хотя бы один прокси")
    created: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    prefix = payload.name_prefix.strip() or "Прокси"
    for index, line in enumerate(lines, 1):
        try:
            created.append(_proxy_safe(create_proxy(f"{prefix} #{index}", line)))
        except (ValueError, TypeError) as exc:
            errors.append({"line": index, "value": line, "error": str(exc)})
    return {"ok": bool(created), "created": created, "errors": errors}


@app.delete("/api/proxies/{proxy_id}")
def api_delete_proxy(proxy_id: int):
    disabled_count = delete_proxy(proxy_id)
    if disabled_count is None:
        raise HTTPException(404, "Прокси не найден")
    return {"ok": True, "disabled_profiles": disabled_count}


# ── Tokens API ────────────────────────────────────────────────────────────────
def _tok_safe(t: Dict) -> Dict:
    return {**t, "api_token_masked": _mask(t["api_token"]), "api_token": "", "proxy_url": ""}

@app.get("/api/tokens")
def api_list_tokens():
    return {"ok": True, "tokens": [_tok_safe(t) for t in get_all_tokens()]}

@app.get("/api/tokens/{tid}")
def api_get_token(tid: int):
    t = get_token(tid)
    if not t: raise HTTPException(404, "Токен не найден")
    return {"ok": True, "token": t}

@app.post("/api/tokens")
def api_create_token(payload: TokenCreate):
    if not payload.api_token.strip(): raise HTTPException(400, "Токен не может быть пустым")
    proxy_id, proxy_url = resolve_proxy_choice(payload.proxy_id, payload.proxy_url)
    t = create_token(payload.name.strip(), payload.login.strip(), payload.api_token.strip(),
                     proxy_url, payload.color.strip(), proxy_id, payload.daily_limit)
    logger.info("Создан токен [T%d] %s", t["id"], t["name"])
    return {"ok": True, "token": _tok_safe(t)}

@app.put("/api/tokens/{tid}")
def api_update_token(tid: int, payload: TokenUpdate):
    raw = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
    provided = getattr(payload, "model_fields_set", None)
    if provided is None:
        provided = getattr(payload, "__fields_set__", set())
    proxy_id = raw.pop("proxy_id", None)
    proxy_url_raw = raw.pop("proxy_url", None)
    fields = {
        k: (v.strip() if isinstance(v, str) else v)
        for k, v in raw.items() if v is not None
    }
    if "proxy_id" in provided or "proxy_url" in provided:
        selected_id, selected_url = resolve_proxy_choice(proxy_id, proxy_url_raw or "")
        fields.update(proxy_id=selected_id, proxy_url=selected_url)
    t = update_token(tid, **fields)
    if not t: raise HTTPException(404, "Токен не найден")
    if "daily_limit" in provided:
        # The chosen applied mode is persistent: changing an account quota must
        # immediately rebalance its enabled, unlocked tasks when Smart mode is on.
        service._maybe_redistribute()
    return {"ok": True, "token": _tok_safe(t)}

@app.delete("/api/tokens/{tid}")
def api_delete_token(tid: int):
    delete_token(tid); return {"ok": True}


# ── Profiles API ──────────────────────────────────────────────────────────────
def _with_rt(p: Dict) -> Dict:
    rs = _get_rs(p["id"])
    direct_ids = (
        extract_direct_ids(p.get("target_url", ""))
        if profile_target_mode(p) == "direct" else []
    )
    direct_count = len(direct_ids)
    safe_interval = 0
    cooldown_warning = ""
    if direct_count:
        effective_batch = min(p["bump_limit"], direct_count)
        safe_interval = math.ceil(
            DIRECT_ITEM_COOLDOWN_MINUTES * effective_batch / direct_count
        )
        if p["interval_minutes"] < safe_interval:
            cooldown_warning = (
                f"Слишком частое расписание для {direct_count} аккаунтов: при пачке "
                f"{effective_batch} безопасный средний интервал — от {safe_interval} мин. "
                "Задача автоматически пропустит аккаунты, которые поднимались менее часа назад."
            )
    return {
        **p, "api_token": "", "proxy_url": "", "is_running": rs["is_running"],
        "running_trigger": rs["trigger"], "direct_item_count": direct_count,
        "cooldown_min_interval": safe_interval,
        "cooldown_warning": cooldown_warning,
    }

@app.get("/api/profiles")
def api_list_profiles():
    return {"ok": True, "profiles": [_with_rt(p) for p in get_all_profiles()]}

@app.post("/api/profiles")
def api_create_profile(payload: ProfileCreate):
    provided = getattr(payload, "model_fields_set", None)
    if provided is None:
        provided = getattr(payload, "__fields_set__", set())
    selected_proxy_id = payload.proxy_id
    selected_proxy_raw = payload.proxy_url.strip()
    proxy_choice_was_omitted = "proxy_id" not in provided and "proxy_url" not in provided
    if proxy_choice_was_omitted and selected_proxy_id is None and not selected_proxy_raw and payload.token_id:
        token = get_token(payload.token_id)
        if token:
            selected_proxy_id = token.get("proxy_id")
            selected_proxy_raw = token.get("proxy_url", "")
    proxy_id, proxy_url = resolve_proxy_choice(selected_proxy_id, selected_proxy_raw)
    target_mode = normalize_target_mode(payload.target_mode, payload.target_url)
    target_url, bump_limit = normalize_profile_target(
        target_mode, payload.target_url, payload.bump_limit,
    )
    p = create_profile(
        name=payload.name.strip(), token_id=payload.token_id,
        api_token=payload.api_token.strip(), proxy_id=proxy_id, proxy_url=proxy_url,
        target_mode=target_mode, target_url=target_url, interval_locked=payload.interval_locked,
        interval_minutes=payload.interval_minutes, bump_limit=bump_limit,
    )
    logger.info("Создана задача [P%d] %s", p["id"], p["name"])
    return {"ok": True, "profile": _with_rt(p)}

@app.put("/api/profiles/{pid}")
def api_update_profile(pid: int, payload: ProfileUpdate):
    current = get_profile(pid)
    raw = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
    provided = getattr(payload, "model_fields_set", None)
    if provided is None:
        provided = getattr(payload, "__fields_set__", set())
    name_confirmed = bool(raw.pop("name_confirmed", False))
    proxy_id = raw.pop("proxy_id", None)
    proxy_url_raw = raw.pop("proxy_url", None)
    fields: Dict[str, Any] = {}
    for k, v in raw.items():
        if v is None:
            # Explicit null detaches a saved token; an omitted field leaves the
            # current token unchanged.
            if k == "token_id" and k in provided:
                fields[k] = None
            continue
        if k == "name" and not name_confirmed:
            continue
        fields[k] = v.strip() if isinstance(v, str) else v
    if "proxy_id" in provided or "proxy_url" in provided:
        selected_id, selected_url = resolve_proxy_choice(proxy_id, proxy_url_raw or "")
        fields.update(proxy_id=selected_id, proxy_url=selected_url, proxy_missing=0)
    if {"target_mode", "target_url", "bump_limit"} & set(provided):
        target_mode = normalize_target_mode(
            fields.get("target_mode", current.get("target_mode")),
            fields.get("target_url", current.get("target_url", "")),
        )
        target_url, bump_limit = normalize_profile_target(
            target_mode,
            fields.get("target_url", current.get("target_url", "")),
            fields.get("bump_limit", current.get("bump_limit", DEFAULT_LIMIT)),
        )
        fields.update(target_mode=target_mode, target_url=target_url, bump_limit=bump_limit)
    if name_confirmed and not fields.get("name"):
        raise HTTPException(400, "Название задачи не может быть пустым")
    p = update_profile(pid, **fields)
    if {
        "token_id", "interval_locked", "target_mode", "target_url",
        "interval_minutes", "bump_limit",
    } & set(provided):
        service._maybe_redistribute()
    return {"ok": True, "profile": _with_rt(p)}

@app.delete("/api/profiles/{pid}")
def api_delete_profile(pid: int):
    if _get_rs(pid)["is_running"]: raise HTTPException(409, "Задача выполняется")
    delete_profile(pid)
    service._maybe_redistribute()
    return {"ok": True}

@app.post("/api/profiles/{pid}/toggle")
def api_toggle(pid: int, payload: TogglePayload):
    p = get_profile(pid)
    if payload.enabled and p.get("proxy_missing"):
        raise HTTPException(409, "Сначала выбери новый прокси в настройках задачи")
    fields: Dict[str, Any] = {"enabled": int(payload.enabled)}
    if payload.enabled:
        fields["next_run_at"] = iso(utc_now() + timedelta(minutes=p["interval_minutes"]))
    else:
        fields["next_run_at"] = None
    p = update_profile(pid, **fields)
    service._maybe_redistribute()
    return {"ok": True, "profile": _with_rt(p)}

@app.post("/api/profiles/{pid}/run-now")
def api_run_now(pid: int):
    if get_profile(pid).get("proxy_missing"):
        raise HTTPException(409, "Запуск заблокирован: назначенный прокси удалён. Выбери новый прокси.")
    result = service.run_once(pid=pid, trigger_source="manual")
    return {"ok": result.get("ok", False), "result": result}

@app.get("/api/profiles/{pid}/logs")
def api_logs(pid: int, limit: int = 50):
    limit = max(1, min(limit, 200))
    with db() as conn:
        rows = conn.execute("""
            SELECT id,profile_id,started_at,finished_at,trigger_source,status,
                   bumped_count,items_found,http_code,item_ids,message
            FROM run_logs WHERE profile_id=? ORDER BY id DESC LIMIT ?
        """, (pid, limit)).fetchall()
    return {"ok": True, "logs": [dict(r) for r in rows]}

@app.delete("/api/profiles/{pid}/logs")
def api_clear_logs(pid: int):
    with db() as conn:
        r = conn.execute("DELETE FROM run_logs WHERE profile_id=?", (pid,))
    return {"ok": True, "deleted": r.rowcount}

@app.get("/api/profiles/{pid}/stats")
def api_stats(pid: int, hours: int = 24):
    hours = max(1, min(hours, 720))
    since = iso(utc_now() - timedelta(hours=hours))
    with db() as conn:
        row = conn.execute("""
            SELECT COUNT(*) AS launches,
                   COALESCE(SUM(bumped_count),0) AS bumped_total,
                   COALESCE(SUM(CASE WHEN status IN ('ok','partial') THEN 1 ELSE 0 END),0) AS success_runs,
                   COALESCE(SUM(CASE WHEN status='error' THEN 1 ELSE 0 END),0) AS error_runs
            FROM run_logs WHERE profile_id=? AND started_at>=?
        """, (pid, since)).fetchone()
    return {"ok": True, "hours": hours, "stats": dict(row)}
