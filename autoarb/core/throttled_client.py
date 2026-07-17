"""Общий HTTP-клиент: опциональный прокси для LZT и пауза после каждого ответа."""

from __future__ import annotations

import hashlib
import logging
import re
import threading
import time
from collections import deque
from typing import Any
from urllib.parse import urlsplit

import requests


MAX_REQUEST_ATTEMPTS = 3
RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}
logger = logging.getLogger("lzt_control.http")


_RATE_LIMITS: dict[str, tuple[int, float]] = {
    "base_get": (300, 60.0),
    "base_write": (30, 60.0),
    "letters": (5, 60.0),
    "batch": (20, 60.0),
    "market_edit": (1000, 60.0),
    "market_confirm": (1000, 60.0),
    "market_fast": (300, 60.0),
    "market_email": (300, 60.0),
    "market_search": (120, 60.0),
}

_RATE_BUCKET_LABELS: dict[str, str] = {
    "base_get": "Обычные GET",
    "base_write": "Обычные POST / PUT",
    "letters": "Письма Market",
    "batch": "Пакетные запросы",
    "market_edit": "Редактирование лотов",
    "market_confirm": "Подтверждение покупки",
    "market_fast": "Проверки и быстрые сделки",
    "market_email": "Коды с почты",
    "market_search": "Поиск и списки аккаунтов",
}


def _rate_bucket(method: str, url: str) -> tuple[str, int, float]:
    """Choose exactly one LZT counter; special endpoints do not consume base limits."""
    parsed = urlsplit(url)
    path = (parsed.path or "/").rstrip("/").lower() or "/"
    method = str(method or "GET").upper()
    bucket = "base_get" if method == "GET" else "base_write"
    # The public documentation writes many endpoints with the ``/market``
    # prefix, while prod-api also exposes their short variants.  Both forms
    # must hit the same counter.
    if re.fullmatch(r"/(?:market/)?letters2?", path):
        bucket = "letters"
    elif path == "/batch":
        bucket = "batch"
    elif re.fullmatch(r"/(?:market/)?\d+/edit", path):
        bucket = "market_edit"
    elif re.fullmatch(r"/(?:market/)?\d+/confirm-buy", path):
        bucket = "market_confirm"
    elif path in {"/market/fast-sell", "/fast-sell", "/item/fast-sell"} or re.fullmatch(
        r"/(?:market/)?\d+/(?:fast-buy|check-account|goods-check|goods/check)", path
    ):
        bucket = "market_fast"
    elif re.fullmatch(r"/(?:market/)?\d+/email-code", path):
        bucket = "market_email"
    elif (
        re.fullmatch(r"/(?:market/)?user(?:/\d+)?/items", path)
        or re.fullmatch(r"/(?:market/)?user(?:/\d+)?/orders", path)
        or (method == "GET" and re.fullmatch(r"/(?:market/)?\d+", path))
        or (method == "GET" and path == "/market")
    ):
        bucket = "market_search"
    limit, window = _RATE_LIMITS[bucket]
    return bucket, limit, window


def _request_presentation(
    method: str,
    url: str,
    source: str = "",
    payload: Any = None,
) -> dict[str, str]:
    """Describe an API call without exposing its query string or payload."""
    path = (urlsplit(str(url or "")).path or "/").rstrip("/").lower() or "/"
    area = str(source or "").strip() or "LZT API"
    operation = "Запрос к LZT API"
    tone = "general"
    action = str(payload.get("action") or "").casefold() if isinstance(payload, dict) else ""

    if path.endswith("/bump") or (path.endswith("/items/bulk-action") and action == "bump"):
        area, operation, tone = "Поднятие", "Поднятие аккаунта", "bump"
    elif re.search(r"/(?:check-account)$", path):
        area, operation, tone = "Проверка", "Проверка на валид", "check"
    elif path.endswith("/item/fast-sell"):
        area, operation, tone = "Пролив", "Быстрый пролив аккаунта", "proliv"
    elif path.endswith("/refuse-guarantee"):
        area, operation, tone = "Пролив", "Отмена гарантии перед ручным проливом", "proliv"
    elif re.search(r"/(?:goods-check|goods/check)$", path):
        area, operation, tone = "Пролив", "Проверка данных и публикация", "proliv"
    elif path.endswith("/item/add"):
        area, operation, tone = "Пролив", "Создание черновика", "proliv"
    elif re.search(r"/goods/add$", path):
        area, operation, tone = "Пролив", "Подготовка данных аккаунта", "proliv"
    elif re.search(r"/(?:tag|edit-tags)$", path) or (path.endswith("/items/bulk-action") and "tag" in action):
        operation, tone = "Изменение меток", "utility" if "утил" in area.casefold() else "proliv"
    elif re.search(r"/orders$", path):
        operation, tone = "Опрос покупок и гарантий", "sync"
    elif path.endswith("/bulk/items"):
        operation, tone = "Массовое получение аккаунтов", "utility" if "утил" in area.casefold() else "check"
    elif re.search(r"/user(?:/\d+)?/items$", path) or (str(method).upper() == "GET" and path == "/market"):
        operation = "Поиск аккаунтов" if area == "Поднятие" else "Получение списка аккаунтов"
        tone = "bump" if area == "Поднятие" else "sync"
    elif re.search(r"/(?:letters2?|claims?)$", path):
        operation, tone = "Работа с арбитражем", "check"
    elif re.search(r"/\d+/email-code$", path):
        operation, tone = "Получение кода с почты", "check"
    elif re.search(r"/\d+/(?:fast-buy|fast-sell|confirm-buy)$", path):
        operation, tone = "Операция с аккаунтом", "utility"
    elif re.search(r"/\d+$", path):
        operation = "Получение данных аккаунта"
        tone = "utility" if "утил" in area.casefold() else "check"
    elif area == "Поднятие":
        operation, tone = "Запрос поднятия", "bump"
    elif "утил" in area.casefold():
        operation, tone = area, "utility"

    return {"area": area, "operation": operation, "tone": tone}


class _LztRateLimiter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sent_at: dict[tuple[str, str], deque[float]] = {}
        self._remote_windows: dict[tuple[str, str], dict[str, float | int]] = {}
        self._history: dict[tuple[str, str], deque[dict[str, Any]]] = {}
        self._responses: dict[tuple[str, str], deque[tuple[float, int]]] = {}

    @staticmethod
    def _token_key(token: str) -> str:
        """Identify a token without retaining or logging the bearer secret."""
        value = str(token or "").strip()
        if not value:
            return "anonymous"
        return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]

    def acquire(
        self,
        method: str,
        url: str,
        token: str = "",
        *,
        virtual: bool = False,
        source: str = "",
        payload: Any = None,
    ) -> dict[str, Any]:
        bucket, limit, window = _rate_bucket(method, url)
        token_key = self._token_key(token)
        counter_key = (token_key, bucket)
        warned = False
        while True:
            with self._lock:
                now = time.monotonic()
                now_epoch = time.time()
                sent = self._sent_at.setdefault(counter_key, deque())
                while sent and now - sent[0] >= window:
                    sent.popleft()

                remote = self._remote_windows.get(counter_key)
                remote_wait = 0.0
                if remote:
                    reset_at = float(remote.get("reset_at") or 0)
                    if reset_at and reset_at <= now_epoch:
                        self._remote_windows.pop(counter_key, None)
                        remote = None
                    elif int(remote.get("remaining") or 0) <= 0:
                        remote_wait = max(0.01, reset_at - now_epoch) if reset_at else window

                if len(sent) < limit and remote_wait <= 0:
                    sent.append(now)
                    history = self._history.setdefault(counter_key, deque())
                    presentation = _request_presentation(method, url, source, payload)
                    event = {
                        "id": f"{token_key}-{int(now_epoch * 1000)}-{threading.get_ident()}",
                        "at": now_epoch,
                        "method": str(method or "GET").upper(),
                        "endpoint": urlsplit(str(url or "")).path or "/",
                        "bucket": bucket,
                        "bucket_label": _RATE_BUCKET_LABELS.get(bucket, bucket),
                        "virtual": bool(virtual),
                        "area": presentation["area"],
                        "operation": presentation["operation"],
                        "tone": presentation["tone"],
                        "state": "quota" if virtual else "pending",
                        "status_code": None,
                        "result": "Резерв лимита" if virtual else "Выполняется",
                        "duration_ms": None,
                        "quota_units": 0 if virtual else 1,
                        "quota_breakdown": [],
                    }
                    history.append(event)
                    while history and now_epoch - float(history[0].get("at") or 0) >= 3600:
                        history.popleft()
                    if remote is not None:
                        remote["remaining"] = max(0, int(remote.get("remaining") or 0) - 1)
                    return event
                local_wait = max(0.01, window - (now - sent[0])) if len(sent) >= limit else 0.0
                wait_seconds = max(local_wait, remote_wait, 0.01)
            if not warned:
                logger.warning(
                    "[HTTP:LZT] токен %s · лимит %s: %d запросов/%d сек; ожидание %.1f сек",
                    token_key[:6], bucket, limit, int(window), wait_seconds,
                )
                warned = True
            time.sleep(wait_seconds)

    def set_quota_details(
        self,
        event: dict[str, Any],
        nested_buckets: list[str],
    ) -> None:
        if not event:
            return
        counts: dict[str, int] = {}
        for bucket in nested_buckets:
            counts[bucket] = counts.get(bucket, 0) + 1
        with self._lock:
            event["quota_units"] = 1 + len(nested_buckets)
            event["quota_breakdown"] = [
                {
                    "key": key,
                    "label": _RATE_BUCKET_LABELS.get(key, key),
                    "units": units,
                }
                for key, units in counts.items()
            ]

    def fail(self, event: dict[str, Any] | None, error: Any) -> None:
        if not event:
            return
        now_epoch = time.time()
        message = str(error or "Ошибка соединения").replace("\n", " ").strip()
        with self._lock:
            event.update({
                "completed_at": now_epoch,
                "duration_ms": max(0, int(round((now_epoch - float(event.get("at") or now_epoch)) * 1000))),
                "state": "error",
                "status_code": 0,
                "result": message[:240] or "Ошибка соединения",
            })

    def observe(
        self,
        method: str,
        url: str,
        token: str,
        response: Any,
        event: dict[str, Any] | None = None,
    ) -> None:
        """Synchronise local counters with rate-limit metadata returned by LZT.

        This also protects a token when it is being used by another part of the
        application between two local requests.  Only counters and a short
        SHA-256 fingerprint are kept; the bearer token itself is never stored.
        """
        bucket, _limit, window = _rate_bucket(method, url)
        counter_key = (self._token_key(token), bucket)
        rate_info: dict[str, Any] = {}
        try:
            payload = response.json()
        except (ValueError, TypeError, AttributeError):
            payload = None
        if isinstance(payload, dict):
            system_info = payload.get("system_info")
            if isinstance(system_info, dict) and isinstance(system_info.get("rate_limit"), dict):
                rate_info = system_info["rate_limit"]
            elif isinstance(payload.get("rate_limit"), dict):
                rate_info = payload["rate_limit"]

        headers = getattr(response, "headers", {}) or {}
        remaining_raw = rate_info.get("remaining", headers.get("X-RateLimit-Remaining"))
        limit_raw = rate_info.get("limit", headers.get("X-RateLimit-Limit"))
        reset_raw = rate_info.get("reset", headers.get("X-RateLimit-Reset"))
        try:
            remaining = max(0, int(remaining_raw))
        except (TypeError, ValueError):
            remaining = None
        try:
            remote_limit = max(1, int(limit_raw))
        except (TypeError, ValueError):
            remote_limit = _RATE_LIMITS[bucket][0]
        try:
            reset_at = float(reset_raw)
        except (TypeError, ValueError):
            reset_at = 0.0
        now_epoch = time.time()
        if reset_at and reset_at < now_epoch:
            reset_at = 0.0
        status_code = int(getattr(response, "status_code", 0) or 0)
        errors = payload.get("errors") if isinstance(payload, dict) else None
        payload_status = str(payload.get("status") or "").casefold() if isinstance(payload, dict) else ""
        deferred = "retry_request" in str(payload or "").casefold()
        failed = status_code >= 400 or bool(errors) or payload_status in {"error", "failed", "fail"}
        if deferred:
            state = "waiting"
            result = "LZT вернул retry_request — запрос будет повторён"
        elif failed:
            state = "error"
            detail = errors or (payload.get("message") if isinstance(payload, dict) else None)
            result = str(detail or f"HTTP {status_code}").replace("\n", " ")[:240]
        else:
            state = "success"
            result = str(
                (payload.get("message") if isinstance(payload, dict) else None)
                or ("Успешно" if 200 <= status_code < 400 else f"HTTP {status_code}")
            ).replace("\n", " ")[:240]
        if event:
            with self._lock:
                event.update({
                    "completed_at": now_epoch,
                    "duration_ms": max(0, int(round((now_epoch - float(event.get("at") or now_epoch)) * 1000))),
                    "state": state,
                    "status_code": status_code,
                    "result": result,
                    "limit": int(remote_limit),
                    "remaining": remaining,
                    "reset_in": max(0, int(round(reset_at - now_epoch))) if reset_at else None,
                })
        with self._lock:
            responses = self._responses.setdefault(counter_key, deque())
            responses.append((now_epoch, status_code))
            while responses and now_epoch - responses[0][0] >= 3600:
                responses.popleft()

        if remaining is None and status_code != 429:
            return
        if status_code == 429:
            remaining = 0
            if not reset_at:
                reset_at = now_epoch + window
        if not reset_at:
            reset_at = now_epoch + window

        with self._lock:
            existing = self._remote_windows.get(counter_key)
            if existing and abs(float(existing.get("reset_at") or 0) - reset_at) < 1:
                # Never increase a remaining counter: another thread may have
                # reserved requests after this response was produced.
                remaining = min(int(existing.get("remaining") or 0), int(remaining or 0))
            self._remote_windows[counter_key] = {
                "limit": int(remote_limit),
                "remaining": int(remaining or 0),
                "reset_at": reset_at,
            }

    def mark_retry_request(
        self,
        event: dict[str, Any] | None,
        attempt: int,
        max_attempts: int,
    ) -> None:
        if not event:
            return
        next_attempt = min(max_attempts, attempt + 1)
        with self._lock:
            event.update({
                "state": "waiting",
                "retry_attempt": int(attempt),
                "retry_max": int(max_attempts),
                "result": (
                    f"retry_request · попытка {attempt}/{max_attempts} завершена · "
                    f"следующая {next_attempt}/{max_attempts}"
                ),
            })

    def snapshot(self, labels: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
        """Build a token-safe API usage snapshot for the local dashboard."""
        labels = labels or {}
        now_epoch = time.time()
        now_mono = time.monotonic()
        with self._lock:
            token_keys = set(labels)
            token_keys.update(key[0] for key in self._history)
            token_keys.update(key[0] for key in self._remote_windows)
            accounts: list[dict[str, Any]] = []
            monitor_events: list[dict[str, Any]] = []
            for token_key in token_keys:
                account_events: list[dict[str, Any]] = []
                bucket_rows: list[dict[str, Any]] = []
                response_statuses: list[tuple[float, int]] = []

                for bucket, (documented_limit, window) in _RATE_LIMITS.items():
                    counter_key = (token_key, bucket)
                    sent = self._sent_at.setdefault(counter_key, deque())
                    while sent and now_mono - sent[0] >= window:
                        sent.popleft()

                    history = self._history.setdefault(counter_key, deque())
                    while history and now_epoch - float(history[0].get("at") or 0) >= 3600:
                        history.popleft()
                    events = list(history)
                    account_events.extend(events)

                    responses = self._responses.setdefault(counter_key, deque())
                    while responses and now_epoch - responses[0][0] >= 3600:
                        responses.popleft()
                    response_statuses.extend(responses)

                    minute_events = [event for event in events if now_epoch - float(event["at"]) < 60]
                    remote = self._remote_windows.get(counter_key) or {}
                    reset_at = float(remote.get("reset_at") or 0)
                    if reset_at and reset_at <= now_epoch:
                        remote = {}
                        self._remote_windows.pop(counter_key, None)
                        reset_at = 0.0
                    effective_limit = max(1, int(remote.get("limit") or documented_limit))
                    remote_remaining = (
                        max(0, int(remote.get("remaining") or 0)) if remote else None
                    )
                    remote_used = (
                        max(0, effective_limit - remote_remaining)
                        if remote_remaining is not None else 0
                    )
                    used_minute = max(len(minute_events), remote_used)
                    used_hour = len(events)
                    bucket_rows.append({
                        "key": bucket,
                        "label": _RATE_BUCKET_LABELS.get(bucket, bucket),
                        "limit": effective_limit,
                        "window_seconds": int(window),
                        "used_minute": used_minute,
                        "local_minute": len(minute_events),
                        "used_hour": used_hour,
                        "remaining": remote_remaining,
                        "reset_in": max(0, int(round(reset_at - now_epoch))) if reset_at else None,
                        "percent": round(min(100.0, used_minute / effective_limit * 100), 1),
                    })

                real_events = [event for event in account_events if not event.get("virtual")]
                minute_real = [event for event in real_events if now_epoch - float(event["at"]) < 60]
                minute_quota = [event for event in account_events if now_epoch - float(event["at"]) < 60]
                errors_minute = sum(
                    1 for event in minute_real if str(event.get("state")) == "error"
                )
                errors_hour = sum(
                    1 for event in real_events if str(event.get("state")) == "error"
                )

                series = [0] * 60
                for event in real_events:
                    age_minutes = int((now_epoch - float(event["at"])) // 60)
                    if 0 <= age_minutes < 60:
                        series[59 - age_minutes] += 1

                endpoints: dict[tuple[str, str], dict[str, int]] = {}
                for event in real_events:
                    endpoint_key = (str(event.get("method") or "GET"), str(event.get("endpoint") or "/"))
                    row = endpoints.setdefault(endpoint_key, {"minute": 0, "hour": 0})
                    row["hour"] += 1
                    if now_epoch - float(event["at"]) < 60:
                        row["minute"] += 1
                top_endpoints = [
                    {"method": method, "endpoint": endpoint, **counts}
                    for (method, endpoint), counts in sorted(
                        endpoints.items(), key=lambda item: (item[1]["minute"], item[1]["hour"]), reverse=True
                    )[:8]
                ]

                label = labels.get(token_key) or {}
                last_activity = max((float(event["at"]) for event in account_events), default=0.0)
                for event in real_events:
                    started_at = float(event.get("at") or now_epoch)
                    duration_ms = event.get("duration_ms")
                    if duration_ms is None:
                        duration_ms = max(0, int(round((now_epoch - started_at) * 1000)))
                    monitor_events.append({
                        "id": str(event.get("id") or ""),
                        "at": started_at,
                        "completed_at": event.get("completed_at"),
                        "duration_ms": int(duration_ms),
                        "method": str(event.get("method") or "GET"),
                        "endpoint": str(event.get("endpoint") or "/"),
                        "bucket": str(event.get("bucket") or "base_get"),
                        "bucket_label": str(event.get("bucket_label") or "Лимит API"),
                        "area": str(event.get("area") or "LZT API"),
                        "operation": str(event.get("operation") or "Запрос к LZT API"),
                        "tone": str(event.get("tone") or "general"),
                        "state": str(event.get("state") or "pending"),
                        "status_code": event.get("status_code"),
                        "result": str(event.get("result") or "Выполняется")[:240],
                        "quota_units": max(1, int(event.get("quota_units") or 1)),
                        "quota_breakdown": list(event.get("quota_breakdown") or []),
                        "limit": event.get("limit"),
                        "remaining": event.get("remaining"),
                        "reset_in": event.get("reset_in"),
                        "account_key": token_key,
                        "account_name": str(label.get("name") or f"Токен {token_key[:6]}"),
                        "account_color": str(label.get("color") or "#10b981"),
                        "account_source": str(label.get("source") or "Обнаружен в запросах"),
                    })
                bucket_rows.sort(key=lambda row: (row["used_minute"] > 0, row["used_hour"] > 0, row["percent"]), reverse=True)
                accounts.append({
                    "key": token_key,
                    "short_key": token_key[:6],
                    "name": str(label.get("name") or f"Токен {token_key[:6]}"),
                    "color": str(label.get("color") or "#10b981"),
                    "source": str(label.get("source") or "Обнаружен в запросах"),
                    "active": bool(last_activity and now_epoch - last_activity < 15),
                    "last_activity_ago": int(now_epoch - last_activity) if last_activity else None,
                    "requests_minute": len(minute_real),
                    "requests_hour": len(real_events),
                    "quota_minute": len(minute_quota),
                    "quota_hour": len(account_events),
                    "errors_minute": errors_minute,
                    "errors_hour": errors_hour,
                    "series": series,
                    "buckets": bucket_rows,
                    "endpoints": top_endpoints,
                })
            accounts.sort(key=lambda row: (row["active"], row["requests_minute"], row["requests_hour"], row["name"]), reverse=True)
            monitor_events.sort(key=lambda row: float(row["at"]), reverse=True)
            return {
                "generated_at": int(now_epoch),
                "accounts": accounts,
                "events": monitor_events[:160],
            }


_LZT_RATE_LIMITER = _LztRateLimiter()


def _batch_operations(payload: Any) -> list[tuple[str, str]]:
    """Extract nested requests from common ``/batch`` payload variants."""
    if isinstance(payload, list):
        entries = payload
    elif isinstance(payload, dict):
        container = next(
            (payload[key] for key in ("requests", "jobs", "operations", "batch") if isinstance(payload.get(key), (list, dict))),
            [],
        )
        entries = list(container.values()) if isinstance(container, dict) else container
    else:
        entries = []
    operations: list[tuple[str, str]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        endpoint = next(
            (entry.get(key) for key in ("url", "endpoint", "path", "uri") if entry.get(key)),
            None,
        )
        if endpoint:
            operations.append((str(entry.get("method") or entry.get("http_method") or "GET"), str(endpoint)))
    return operations


def wait_for_lzt_rate_limit(
    method: str,
    url: str,
    token: str = "",
    *,
    batch_payload: Any = None,
    request_payload: Any = None,
    source: str = "",
) -> dict[str, Any]:
    """Shared per-token limiter for every official LZT API request.

    A ``/batch`` request consumes its own 20/min counter.  Each nested request
    is reserved separately as required by LZT's independent endpoint limits.
    """
    payload = request_payload if request_payload is not None else batch_payload
    event = _LZT_RATE_LIMITER.acquire(
        method, url, token, source=source, payload=payload,
    )
    nested_buckets: list[str] = []
    if _rate_bucket(method, url)[0] == "batch":
        for nested_method, nested_url in _batch_operations(batch_payload):
            nested_bucket = _rate_bucket(nested_method, nested_url)[0]
            nested_buckets.append(nested_bucket)
            _LZT_RATE_LIMITER.acquire(
                nested_method, nested_url, token, virtual=True,
                source=source, payload=None,
            )
    _LZT_RATE_LIMITER.set_quota_details(event, nested_buckets)
    return event


def observe_lzt_rate_limit(
    method: str,
    url: str,
    token: str,
    response: Any,
    request_event: dict[str, Any] | None = None,
) -> None:
    """Feed LZT's returned ``system_info.rate_limit`` into the shared limiter."""
    _LZT_RATE_LIMITER.observe(method, url, token, response, request_event)


def fail_lzt_api_request(request_event: dict[str, Any] | None, error: Any) -> None:
    """Finish a monitor row when no HTTP response was received."""
    _LZT_RATE_LIMITER.fail(request_event, error)


def mark_lzt_retry_request(
    response: Any,
    attempt: int,
    max_attempts: int,
) -> None:
    """Annotate a completed HTTP 200 row with fast-sell retry progress."""
    event = getattr(response, "_lzt_request_event", None)
    _LZT_RATE_LIMITER.mark_retry_request(event, attempt, max_attempts)


def lzt_token_fingerprint(token: str) -> str:
    """Return the same irreversible short key that the limiter uses."""
    return _LztRateLimiter._token_key(token)


def get_lzt_api_monitor(labels: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    """Return usage metrics without exposing any bearer token."""
    return _LZT_RATE_LIMITER.snapshot(labels)


def _endpoint(url: str) -> str:
    """Return a useful endpoint without leaking query parameters or secrets."""
    parsed = urlsplit(url)
    return f"{parsed.netloc}{parsed.path}" if parsed.netloc else parsed.path or url


def use_lzt_proxy_from_config(config: dict) -> tuple[bool, str]:
    """
    use_proxy: false — без прокси (запросы напрямую).
    use_proxy: true — прокси, если строка proxy непустая.
    Ключ use_proxy отсутствует — как раньше: прокси только если proxy непустой.
    """
    proxy_str = str(config.get("proxy") or "").strip()
    explicit = config.get("use_proxy")
    if explicit is False:
        return False, proxy_str
    if explicit is True:
        return bool(proxy_str), proxy_str
    return bool(proxy_str), proxy_str


class ThrottledClient:
    def __init__(
        self,
        token: str,
        proxy: str = "",
        *,
        delay_seconds: float = 3.0,
        use_lzt_proxy: bool = True,
        source: str = "",
    ) -> None:
        self._token = str(token or "").strip()
        self._delay = max(0.0, float(delay_seconds))
        self._source = str(source or "").strip()
        self._lock = threading.Lock()
        self._headers = {
            "accept": "application/json",
            "authorization": f"Bearer {token}",
            "user-agent": "LZT-Control-Check/4",
        }
        p = str(proxy or "").strip()
        if use_lzt_proxy and p:
            proxy_url = p if p.startswith(("http://", "https://", "socks4://", "socks5://")) else f"http://{p}"
            self._proxies: dict[str, str] | None = {
                "https": proxy_url,
                "http": proxy_url,
            }
        else:
            self._proxies = None
        self._session = requests.Session()

    def _sleep_after(self) -> None:
        if self._delay:
            time.sleep(self._delay)

    def _request(
        self,
        method: str,
        url: str,
        *,
        use_proxy: bool,
        retry_safe: bool = False,
        **kwargs: Any,
    ) -> requests.Response:
        """Serialize calls and retry only operations that are safe to repeat.

        Mutating POST requests are not retried by default: if a connection drops
        after the server accepted a publish request, repeating it could create a
        duplicate. Read-only POST endpoints may explicitly opt in with
        ``retry_safe=True`` (for example, bulk statistics lookups).
        """
        attempts = MAX_REQUEST_ATTEMPTS if retry_safe else 1
        proxies = self._proxies if (use_proxy and self._proxies) else None
        endpoint = _endpoint(url)
        with self._lock:
            for attempt in range(1, attempts + 1):
                request_event: dict[str, Any] | None = None
                try:
                    batch_payload = kwargs.get("json") if _rate_bucket(method, url)[0] == "batch" else None
                    request_payload = kwargs.get("json") if kwargs.get("json") is not None else kwargs.get("data")
                    request_event = wait_for_lzt_rate_limit(
                        method,
                        url,
                        self._token,
                        batch_payload=batch_payload,
                        request_payload=request_payload,
                        source=self._source,
                    )
                    request_timeout = (12, 300) if urlsplit(str(url)).path.rstrip("/").endswith("/item/fast-sell") else (12, 60)
                    response = self._session.request(
                        method,
                        url,
                        headers=self._headers,
                        proxies=proxies,
                        timeout=request_timeout,
                        **kwargs,
                    )
                    observe_lzt_rate_limit(method, url, self._token, response, request_event)
                    try:
                        response._lzt_request_event = request_event
                    except Exception:
                        pass
                    self._sleep_after()
                    if retry_safe and response.status_code in RETRYABLE_STATUS_CODES and attempt < attempts:
                        retry_after = response.headers.get("Retry-After", "")
                        try:
                            delay = float(retry_after)
                        except (TypeError, ValueError):
                            delay = float(attempt)
                        delay = max(0.25, min(10.0, delay))
                        logger.warning(
                            "[HTTP:LZT] %s %s → HTTP %d; повтор %d/%d через %.1f с",
                            method, endpoint, response.status_code, attempt + 1, attempts, delay,
                        )
                        response.close()
                        time.sleep(delay)
                        continue
                    if response.status_code >= 400:
                        suffix = (
                            f"; попытки исчерпаны: {attempts}"
                            if retry_safe and response.status_code in RETRYABLE_STATUS_CODES
                            else ""
                        )
                        logger.warning(
                            "[HTTP:LZT] %s %s → HTTP %d%s",
                            method, endpoint, response.status_code, suffix,
                        )
                    elif attempt > 1:
                        logger.info(
                            "[HTTP:LZT] %s %s → успешно с попытки %d/%d",
                            method, endpoint, attempt, attempts,
                        )
                    return response
                except requests.RequestException as exc:
                    fail_lzt_api_request(request_event, exc)
                    if attempt >= attempts:
                        logger.error(
                            "[HTTP:LZT] %s %s → нет ответа после %d попыток: %s",
                            method, endpoint, attempts, exc,
                        )
                        raise
                    delay = float(attempt)
                    logger.warning(
                        "[HTTP:LZT] %s %s → %s; повтор %d/%d через %.0f с",
                        method, endpoint, type(exc).__name__, attempt + 1, attempts, delay,
                    )
                    time.sleep(delay)
        raise RuntimeError("HTTP request failed without a response")

    def get(self, url: str, *, use_proxy: bool = True) -> requests.Response:
        return self._request("GET", url, use_proxy=use_proxy, retry_safe=True)

    def post(
        self,
        url: str,
        *,
        data: dict[str, Any] | None = None,
        json_body: dict | None = None,
        params: dict[str, Any] | None = None,
        use_proxy: bool = True,
        retry_safe: bool = False,
    ) -> requests.Response:
        return self._request(
            "POST",
            url,
            use_proxy=use_proxy,
            retry_safe=retry_safe,
            data=data,
            json=json_body,
            params=params,
        )

    def post_plain(
        self,
        url: str,
        *,
        data: dict[str, Any] | None = None,
        use_proxy: bool = False,
    ) -> requests.Response:
        """POST без LZT-заголовков (Telegram и т.п.); пауза после ответа та же."""
        # Telegram must not receive the LZT bearer header.
        proxies = self._proxies if (use_proxy and self._proxies) else None
        with self._lock:
            try:
                r = requests.post(url, data=data, proxies=proxies, timeout=(12, 60))
            except requests.RequestException as exc:
                logger.error("[HTTP:Telegram] POST %s → нет ответа: %s", _endpoint(url), exc)
                raise
            self._sleep_after()
            if r.status_code >= 400:
                logger.warning("[HTTP:Telegram] POST %s → HTTP %d", _endpoint(url), r.status_code)
            return r
