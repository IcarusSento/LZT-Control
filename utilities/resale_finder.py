from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import parse_qs, urlencode, urlsplit

from autoarb.core.lzt_common import api_base
from autoarb.core.throttled_client import ThrottledClient, use_lzt_proxy_from_config


logger = logging.getLogger("lzt_control.utilities.resale_finder")
ALLOWED_LINK_HOSTS = {
    "lzt.market", "www.lzt.market", "zelenka.guru", "www.zelenka.guru",
    "lolz.guru", "www.lolz.guru", "api.lzt.market", "prod-api.lzt.market",
}
SAME_ID_KEYS = ("sameItemsIds", "sameItemIds", "same_items_ids")
BULK_ITEMS_LIMIT = 250
BULK_TAGS_LIMIT = 5000
POST_REQUESTS_PER_MINUTE = 30
POST_RATE_WINDOW_SECONDS = 60.0
POST_RATE_LIMIT_WAIT_SECONDS = 60.0
POST_RETRY_ATTEMPTS = 3
POST_RETRY_WAIT_SECONDS = 5.0
POST_RATE_LIMIT_RETRIES = 5
KNOWN_ITEM_STATES = {
    "active", "deleted", "closed", "awaiting", "closed_inactive", "paid",
}
LOSS_STATES = {"deleted", "awaiting"}
NEUTRAL_STATES = {"closed", "closed_inactive"}
STORED_ROW_KEYS = {
    "purchase_id", "purchase_price", "purchase_date", "resale_id", "resale_ids",
    "previous_ids", "subsequent_ids", "sold", "page",
    "resale_state", "resale_price", "resale_checked_at", "resale_error",
    "resale_published_date", "financial_class", "financial_result", "profit_percent",
}


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def chunks(values: list[Any], size: int) -> Iterable[list[Any]]:
    if size < 1:
        raise ValueError("Размер пакета должен быть больше нуля")
    for index in range(0, len(values), size):
        yield values[index:index + size]


def ru_count(value: int, one: str, few: str, many: str) -> str:
    number = abs(int(value))
    tail = number % 100
    if 11 <= tail <= 19:
        word = many
    else:
        last = number % 10
        word = one if last == 1 else few if 2 <= last <= 4 else many
    return f"{number} {word}"


class PostRateLimiter:
    """Rolling POST limit shared by all batches of one utility operation."""

    def __init__(self, limit: int = POST_REQUESTS_PER_MINUTE, window: float = POST_RATE_WINDOW_SECONDS) -> None:
        self.limit = max(1, int(limit))
        self.window = max(0.01, float(window))
        self._sent_at: deque[float] = deque()

    def acquire(
        self,
        cancel_event: threading.Event,
        on_wait: Callable[[int], None] | None = None,
    ) -> bool:
        while not cancel_event.is_set():
            now = time.monotonic()
            while self._sent_at and now - self._sent_at[0] >= self.window:
                self._sent_at.popleft()
            if len(self._sent_at) < self.limit:
                self._sent_at.append(now)
                return True
            wait_seconds = max(0.01, self.window - (now - self._sent_at[0]))
            if on_wait:
                on_wait(max(1, math.ceil(wait_seconds)))
            if cancel_event.wait(wait_seconds):
                return False
        return False


def build_orders_url(orders_url: str, base_url: str, page: int) -> str:
    """Build one page request that embeds sameItemsIds in every returned item."""
    parsed = urlsplit(str(orders_url or "").strip())
    if parsed.scheme != "https" or parsed.hostname not in ALLOWED_LINK_HOSTS:
        raise ValueError("Нужна официальная HTTPS-ссылка LZT Market на страницу покупок")
    if "/orders" not in parsed.path.rstrip("/"):
        raise ValueError("В ссылке не найден раздел покупок /orders")
    if page < 1:
        raise ValueError("Номер страницы должен быть больше нуля")
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["page"] = [str(page)]
    params.setdefault("order_by", ["pdate_to_down"])
    params["parse_same_item_ids"] = ["true"]
    return f"{base_url}/user/orders?{urlencode(params, doseq=True)}"


def extract_items(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise ValueError("LZT API не вернул массив покупок items")
    return [item for item in payload["items"] if isinstance(item, dict)]


def _same_ids(payload: dict[str, Any], item: dict[str, Any], purchase_id: int) -> list[int]:
    raw: Any = None
    for key in SAME_ID_KEYS:
        if key in item:
            raw = item.get(key)
            break
    if raw is None:
        for key in ("sameItemsIdsByItem", "sameItemIdsByItem", "same_items_ids_by_item"):
            mapping = payload.get(key)
            if isinstance(mapping, dict):
                raw = mapping.get(str(purchase_id))
                if raw is None:
                    raw = mapping.get(purchase_id)
                if raw is not None:
                    break
    result: set[int] = set()
    if isinstance(raw, list):
        for value in raw:
            try:
                number = int(value)
            except (TypeError, ValueError):
                continue
            if number > 0:
                result.add(number)
    return sorted(result)


def _money(value: Any) -> int | float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        number = float(str(value).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number < 0:
        return None
    return int(number) if number.is_integer() else round(number, 2)


def _unix_date(value: Any) -> int | None:
    try:
        timestamp = int(float(value))
    except (TypeError, ValueError):
        return None
    if timestamp <= 0:
        return None
    # Defensive support for millisecond timestamps, although LZT currently
    # returns operation_date and published_date in seconds.
    if timestamp > 10_000_000_000:
        timestamp //= 1000
    return timestamp


def safe_result_from_item(payload: dict[str, Any], item: dict[str, Any], page: int) -> dict[str, Any] | None:
    """Reduce a purchase to non-secret result fields; raw account data is discarded."""
    raw_id = item.get("item_id") or item.get("itemId") or item.get("id")
    try:
        purchase_id = int(raw_id)
    except (TypeError, ValueError):
        return None
    same_ids = _same_ids(payload, item, purchase_id)
    previous_ids = [value for value in same_ids if value < purchase_id]
    newer_ids = [value for value in same_ids if value > purchase_id]
    first_resale = min(newer_ids) if newer_ids else None
    subsequent_ids = [value for value in newer_ids if first_resale and value > first_resale]
    raw_price = item.get("rub_price")
    if raw_price in (None, ""):
        raw_price = item.get("price")
    buyer = item.get("buyer") if isinstance(item.get("buyer"), dict) else {}
    purchase_date = _unix_date(
        buyer.get("operation_date") or item.get("operation_date") or item.get("paid_date")
    )
    return {
        "purchase_id": str(purchase_id),
        "purchase_price": _money(raw_price),
        "purchase_date": purchase_date,
        "resale_id": str(first_resale) if first_resale else None,
        "resale_ids": [str(value) for value in newer_ids],
        "previous_ids": [str(value) for value in previous_ids],
        "subsequent_ids": [str(value) for value in subsequent_ids],
        "sold": bool(first_resale),
        "page": page,
    }


def _bulk_containers(payload: Any) -> list[Any]:
    if not isinstance(payload, dict):
        return []
    containers: list[Any] = []
    for key in ("items", "results"):
        if key in payload:
            containers.append(payload.get(key))
    data = payload.get("data")
    if isinstance(data, dict):
        if "items" in data:
            containers.append(data.get("items"))
        containers.append(data)
    elif isinstance(data, list):
        containers.append(data)
    return containers


def extract_bulk_item_map(payload: Any) -> dict[str, dict[str, Any]]:
    """Accept list- and ID-keyed variants returned by /bulk/items."""
    result: dict[str, dict[str, Any]] = {}
    for container in _bulk_containers(payload):
        pairs: list[tuple[Any, Any]]
        if isinstance(container, list):
            pairs = [(None, value) for value in container]
        elif isinstance(container, dict):
            pairs = list(container.items())
        else:
            continue
        for key, value in pairs:
            if not isinstance(value, dict):
                continue
            # The production /bulk/items response is ID-keyed and wraps the
            # actual account as items[id].item. Older API variants returned the
            # account directly, so keep both shapes supported.
            if isinstance(value.get("item"), dict):
                item = dict(value["item"])
                # Some /bulk/items responses keep the resale chain next to
                # the wrapped item instead of inside it. Preserve only these
                # non-secret relation fields for downstream analysis.
                for relation_key in ("sameItemsIds", "sameItemIds", "same_items_ids"):
                    if relation_key not in item and relation_key in value:
                        item[relation_key] = value.get(relation_key)
            else:
                item = value
            raw_id = item.get("item_id") or item.get("itemId") or item.get("id") or key
            try:
                item_id = str(int(raw_id))
            except (TypeError, ValueError):
                continue
            result[item_id] = item
        if result:
            break
    return result


def safe_bulk_detail_from_item(item: dict[str, Any]) -> dict[str, Any]:
    """Return only the status and price needed by the report."""
    state = str(item.get("item_state") or item.get("state") or "unknown").strip().lower()
    if state not in KNOWN_ITEM_STATES:
        state = "unknown"
    raw_price = item.get("rub_price")
    if raw_price in (None, ""):
        raw_price = item.get("price")
    raw_item_id = item.get("item_id") or item.get("itemId") or item.get("id") or 0
    try:
        item_id = int(raw_item_id)
    except (TypeError, ValueError):
        item_id = 0
    same_item_ids = _same_ids({}, item, item_id)
    return {
        "resale_state": state,
        "resale_price": _money(raw_price),
        "resale_published_date": _unix_date(item.get("published_date")),
        "same_item_ids": [str(value) for value in same_item_ids],
    }


def enrich_financial_fields(row: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    state = str(detail.get("resale_state") or "unknown")
    purchase = _money(row.get("purchase_price"))
    resale = _money(detail.get("resale_price"))
    financial_class = "unknown"
    financial_result: int | float | None = None
    if state == "paid":
        financial_class = "profit"
        if purchase is not None and resale is not None:
            financial_result = round(float(resale) - float(purchase), 2)
    elif state == "active":
        financial_class = "potential"
        if purchase is not None and resale is not None:
            financial_result = round(float(resale) - float(purchase), 2)
    elif state in LOSS_STATES:
        financial_class = "loss"
        if purchase is not None:
            financial_result = -round(float(purchase), 2)
    elif state in NEUTRAL_STATES:
        financial_class = "neutral"
        financial_result = 0
    profit_percent: float | None = None
    if purchase not in (None, 0) and state in {"paid", "active"} and resale is not None:
        profit_percent = round((float(resale) - float(purchase)) / float(purchase) * 100, 2)
    elif purchase not in (None, 0) and state in LOSS_STATES:
        profit_percent = -100.0
    elif purchase not in (None, 0) and state in NEUTRAL_STATES:
        profit_percent = 0.0
    same_item_ids: list[int] = []
    for value in detail.get("same_item_ids") or []:
        try:
            same_item_ids.append(int(value))
        except (TypeError, ValueError):
            continue
    try:
        purchase_id = int(row.get("purchase_id") or 0)
    except (TypeError, ValueError):
        purchase_id = 0
    try:
        resale_id = int(row.get("resale_id") or 0)
    except (TypeError, ValueError):
        resale_id = 0
    if same_item_ids and purchase_id:
        result["previous_ids"] = [str(value) for value in sorted(set(same_item_ids)) if value < purchase_id]
        result["resale_ids"] = [str(value) for value in sorted(set(same_item_ids)) if value > purchase_id]
        if resale_id:
            result["subsequent_ids"] = [
                str(value) for value in sorted(set(same_item_ids)) if value > resale_id
            ]
    result.update({
        "resale_state": state,
        "resale_price": resale,
        "resale_published_date": detail.get("resale_published_date"),
        "resale_checked_at": utc_iso(),
        "resale_error": None,
        "financial_class": financial_class,
        "financial_result": financial_result,
        "profit_percent": profit_percent,
    })
    return result


def selection_matches(row: dict[str, Any], selection: str) -> bool:
    if selection == "all":
        return True
    if selection == "sold":
        return bool(row.get("sold"))
    if selection == "unsold":
        return not row.get("sold")
    if selection.startswith("state:"):
        return str(row.get("resale_state") or "unknown") == selection.split(":", 1)[1]
    return False


def selected_purchase_ids(rows: list[dict[str, Any]], selections: list[str]) -> list[int]:
    """Return the union of selected groups without duplicate purchase IDs."""
    return list(dict.fromkeys(
        int(row["purchase_id"])
        for row in rows
        if row.get("purchase_id") and any(selection_matches(row, value) for value in selections)
    ))


def _safe_api_error(response: Any) -> str:
    try:
        payload = response.json()
    except (ValueError, TypeError):
        payload = None
    if isinstance(payload, dict):
        for key in ("error_description", "message", "error", "errors", "detail"):
            value = payload.get(key)
            if value not in (None, "", [], {}):
                rendered = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
                return rendered[:320]
    return f"HTTP {getattr(response, 'status_code', '?')}"


def _is_rate_limit_response(response: Any, message: str) -> bool:
    if int(getattr(response, "status_code", 0) or 0) == 429:
        return True
    text = str(message or "").lower()
    return any(marker in text for marker in ("rate limit", "too many requests", "лимит запрос"))


def financial_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    states = Counter(str(row.get("resale_state") or "unknown") for row in rows if row.get("sold"))
    invested = sum(
        float(row["purchase_price"])
        for row in rows if isinstance(row.get("purchase_price"), (int, float))
    )
    realized_revenue = sum(
        float(row["resale_price"])
        for row in rows
        if row.get("resale_state") == "paid" and isinstance(row.get("resale_price"), (int, float))
    )
    active_listings = sum(
        float(row["resale_price"])
        for row in rows
        if row.get("resale_state") == "active" and isinstance(row.get("resale_price"), (int, float))
    )
    deleted_loss = sum(
        float(row["purchase_price"])
        for row in rows
        if row.get("resale_state") == "deleted" and isinstance(row.get("purchase_price"), (int, float))
    )
    realized_result = realized_revenue - invested
    potential_result = realized_revenue + active_listings - invested
    return {
        "states": dict(states),
        "details_ready": sum(states.values()),
        "invested": round(invested, 2),
        "realized_revenue": round(realized_revenue, 2),
        "realized_result": round(realized_result, 2),
        "loss_value": round(deleted_loss, 2),
        "net_result": round(realized_result, 2),
        "active_listing_value": round(active_listings, 2),
        "potential_result": round(potential_result, 2),
        "realized_percent": round(realized_result / invested * 100, 2) if invested else None,
        "potential_percent": round(potential_result / invested * 100, 2) if invested else None,
    }


class ResaleFinderService:
    def __init__(self, config_loader: Callable[[], dict[str, Any]], state_path: Path) -> None:
        self._config_loader = config_loader
        self._state_path = state_path
        self._lock = threading.RLock()
        self._cancel = threading.Event()
        self._details_cancel = threading.Event()
        self._tags_cancel = threading.Event()
        self._thread: threading.Thread | None = None
        self._details_thread: threading.Thread | None = None
        self._tags_thread: threading.Thread | None = None
        self._logs: deque[dict[str, str]] = deque(maxlen=300)
        self._results: list[dict[str, Any]] = []
        self._revision = 0
        self._state: dict[str, Any] = {
            "running": False, "status": "idle", "error": None,
            "started_at": None, "finished_at": None,
            "page_from": 1, "page_to": 3, "current_page": None,
            "pages_done": 0, "total_pages": 0, "api_requests": 0,
        }
        self._details_state = self._empty_operation_state()
        self._tags_state = self._empty_operation_state()
        self._load_persisted()

    @staticmethod
    def _empty_operation_state() -> dict[str, Any]:
        return {
            "running": False, "status": "idle", "error": None,
            "started_at": None, "finished_at": None,
            "total_items": 0, "processed_items": 0,
            "total_batches": 0, "batches_done": 0, "api_requests": 0,
            "log_start": 0,
        }

    def _any_running(self) -> bool:
        return bool(self._state["running"] or self._details_state["running"] or self._tags_state["running"])

    def _load_persisted(self) -> None:
        if not self._state_path.is_file():
            return
        try:
            raw = json.loads(self._state_path.read_text(encoding="utf-8"))
            results = raw.get("results") if isinstance(raw, dict) else None
            if isinstance(results, list):
                self._results = [
                    {key: row.get(key) for key in STORED_ROW_KEYS if key in row}
                    for row in results if isinstance(row, dict)
                ]
                self._revision = 1
                self._state.update({
                    "status": "completed" if self._results else "idle",
                    "finished_at": raw.get("finished_at"),
                    "page_from": int(raw.get("page_from") or 1),
                    "page_to": int(raw.get("page_to") or 3),
                    "pages_done": int(raw.get("pages_done") or 0),
                    "total_pages": int(raw.get("total_pages") or 0),
                    "api_requests": int(raw.get("api_requests") or 0),
                })
                detailed = sum(1 for row in self._results if row.get("resale_state"))
                if detailed:
                    sold = sum(1 for row in self._results if row.get("sold"))
                    self._details_state.update({
                        "status": "completed", "finished_at": raw.get("details_finished_at"),
                        "total_items": sold, "processed_items": detailed,
                        "total_batches": math.ceil(sold / BULK_ITEMS_LIMIT),
                        "batches_done": math.ceil(detailed / BULK_ITEMS_LIMIT),
                    })
        except (OSError, ValueError, TypeError):
            logger.warning("[UTILITIES] Не удалось прочитать сохранённый результат поиска перепродаж")

    def _persist(self) -> None:
        with self._lock:
            payload = {
                "schema_version": 4,
                "finished_at": self._state.get("finished_at"),
                "details_finished_at": self._details_state.get("finished_at"),
                "page_from": self._state.get("page_from"),
                "page_to": self._state.get("page_to"),
                "pages_done": self._state.get("pages_done"),
                "total_pages": self._state.get("total_pages"),
                "api_requests": self._state.get("api_requests"),
                "results": [
                    {key: row.get(key) for key in STORED_ROW_KEYS if key in row}
                    for row in self._results
                ],
            }
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        temp = self._state_path.with_suffix(".tmp")
        temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temp, self._state_path)

    def _log(self, message: str, level: str = "info") -> None:
        entry = {"at": datetime.now().strftime("%H:%M:%S"), "level": level, "message": message}
        with self._lock:
            self._logs.append(entry)
        log_method = logger.error if level == "error" else logger.warning if level == "warning" else logger.info
        log_method("[UTILITIES] %s", message)

    def _client(self, cfg: dict[str, Any]) -> tuple[ThrottledClient, bool]:
        enabled_proxy, proxy = use_lzt_proxy_from_config(cfg)
        return ThrottledClient(
            str(cfg.get("token") or ""), proxy,
            delay_seconds=max(3.0, float(cfg.get("request_delay_seconds", 3) or 3)),
            use_lzt_proxy=enabled_proxy,
            source="Утилита: аудит покупок",
        ), enabled_proxy

    def _post_batch(
        self,
        *,
        client: ThrottledClient,
        endpoint: str,
        json_body: dict[str, Any],
        enabled_proxy: bool,
        operation: dict[str, Any],
        cancel_event: threading.Event,
        limiter: PostRateLimiter,
        batch_label: str,
    ) -> Any | None:
        attempt = 0
        rate_limit_retries = 0
        while not cancel_event.is_set():
            allowed = limiter.acquire(
                cancel_event,
                lambda seconds: self._log(
                    f"Лимит POST-запросов: пауза {seconds} сек. {batch_label} продолжится автоматически.",
                    "warning",
                ),
            )
            if not allowed:
                return None
            try:
                response = client.post(
                    endpoint,
                    json_body=json_body,
                    use_proxy=enabled_proxy,
                    retry_safe=False,
                )
            except Exception as exc:
                attempt += 1
                if attempt >= POST_RETRY_ATTEMPTS:
                    raise ValueError(
                        f"{batch_label}: запрос не выполнен после {POST_RETRY_ATTEMPTS} попыток — {exc}"
                    ) from exc
                self._log(
                    f"{batch_label}: ошибка соединения. Повтор {attempt + 1}/{POST_RETRY_ATTEMPTS} "
                    f"этой же пачки через {POST_RETRY_WAIT_SECONDS:g} сек.",
                    "warning",
                )
                if cancel_event.wait(POST_RETRY_WAIT_SECONDS):
                    return None
                continue
            with self._lock:
                operation["api_requests"] += 1
            if 200 <= response.status_code < 300:
                return response
            api_error = _safe_api_error(response)
            if _is_rate_limit_response(response, api_error):
                rate_limit_retries += 1
                if rate_limit_retries > POST_RATE_LIMIT_RETRIES:
                    raise ValueError(f"{batch_label}: лимит API не снялся после ожидания — {api_error}")
                self._log(
                    f"{batch_label}: LZT API сообщил о лимите. Ждём {POST_RATE_LIMIT_WAIT_SECONDS:g} сек "
                    "и повторяем эту же пачку.",
                    "warning",
                )
                if cancel_event.wait(POST_RATE_LIMIT_WAIT_SECONDS):
                    return None
                continue
            attempt += 1
            if attempt >= POST_RETRY_ATTEMPTS:
                raise ValueError(
                    f"{batch_label}: API отклонил запрос после {POST_RETRY_ATTEMPTS} попыток — {api_error}"
                )
            self._log(
                f"{batch_label}: API вернул ошибку ({api_error}). Повтор {attempt + 1}/{POST_RETRY_ATTEMPTS} "
                f"этой же пачки через {POST_RETRY_WAIT_SECONDS:g} сек.",
                "warning",
            )
            if cancel_event.wait(POST_RETRY_WAIT_SECONDS):
                return None
        return None

    def _config(self, token_override: str = "") -> dict[str, Any]:
        # A token entered on the utility page lives only in this operation's
        # in-memory config copy. It is never written to settings or state JSON.
        cfg = dict(self._config_loader())
        temporary_token = str(token_override or "").strip()
        if temporary_token:
            cfg["token"] = temporary_token
        if not str(cfg.get("token") or "").strip():
            raise ValueError("Укажи API-токен в утилите или в настройках проверки")
        return cfg

    def defaults(self) -> dict[str, Any]:
        cfg = self._config_loader()
        return {
            "ok": True,
            # Purchase links belong to this utility. They are intentionally not
            # borrowed from the checks page because those filters serve a
            # different workflow.
            "orders_url": "",
            "request_interval_seconds": max(3.0, float(cfg.get("request_delay_seconds", 3) or 3)),
            "bulk_items_limit": BULK_ITEMS_LIMIT,
            "bulk_tags_limit": BULK_TAGS_LIMIT,
            "post_requests_per_minute": POST_REQUESTS_PER_MINUTE,
        }

    def start(self, orders_url: str, page_from: int, page_to: int, token_override: str = "") -> dict[str, Any]:
        if page_from < 1 or page_to < page_from:
            raise ValueError("Проверь диапазон страниц")
        if page_to - page_from + 1 > 100:
            raise ValueError("За один запуск можно проверить не больше 100 страниц")
        cfg = self._config(token_override)
        source_url = str(orders_url or "").strip()
        base_url = api_base(cfg)
        build_orders_url(source_url, base_url, page_from)
        with self._lock:
            if self._any_running():
                raise RuntimeError("Дождись завершения текущей операции утилиты")
            self._cancel.clear()
            self._results = []
            self._logs.clear()
            self._revision += 1
            self._details_state = self._empty_operation_state()
            self._tags_state = self._empty_operation_state()
            self._state.update({
                "running": True, "status": "running", "error": None,
                "started_at": utc_iso(), "finished_at": None,
                "page_from": page_from, "page_to": page_to, "current_page": page_from,
                "pages_done": 0, "total_pages": page_to - page_from + 1, "api_requests": 0,
            })
            self._thread = threading.Thread(
                target=self._run,
                args=(cfg, source_url, base_url, page_from, page_to),
                name="utility-resale-finder",
                daemon=True,
            )
            self._thread.start()
        return self.status()

    def _run(self, cfg: dict[str, Any], source_url: str, base_url: str, page_from: int, page_to: int) -> None:
        try:
            client, enabled_proxy = self._client(cfg)
            seen: set[str] = set()
            self._log(f"Поиск начат · страницы {page_from}–{page_to} · один запрос на страницу")
            for page in range(page_from, page_to + 1):
                if self._cancel.is_set():
                    break
                with self._lock:
                    self._state["current_page"] = page
                self._log(f"Страница {page} из {page_to} · получаем покупки")
                url = build_orders_url(source_url, base_url, page)
                response = client.get(url, use_proxy=enabled_proxy)
                with self._lock:
                    self._state["api_requests"] += 1
                if response.status_code != 200:
                    if response.status_code in {401, 403}:
                        raise ValueError(f"LZT API отклонил токен или доступ (HTTP {response.status_code})")
                    raise ValueError(f"LZT API вернул HTTP {response.status_code} на странице {page}: {_safe_api_error(response)}")
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise ValueError(f"LZT API вернул повреждённый JSON на странице {page}") from exc
                items = extract_items(payload)
                page_rows: list[dict[str, Any]] = []
                for item in items:
                    row = safe_result_from_item(payload, item, page)
                    if row and row["purchase_id"] not in seen:
                        seen.add(row["purchase_id"])
                        page_rows.append(row)
                with self._lock:
                    self._results.extend(page_rows)
                    self._state["pages_done"] += 1
                    self._revision += 1
                sold = sum(1 for row in page_rows if row["sold"])
                self._log(
                    f"Страница {page} готова · {ru_count(len(page_rows), 'покупка', 'покупки', 'покупок')} · "
                    f"{sold} продано · {len(page_rows) - sold} не продано"
                )
                self._persist()
            with self._lock:
                cancelled = self._cancel.is_set()
                self._state.update({
                    "running": False, "status": "cancelled" if cancelled else "completed",
                    "current_page": None, "finished_at": utc_iso(),
                })
            self._log("Поиск остановлен · найденные результаты сохранены", "warning") if cancelled else self._log(
                f"Поиск завершён · обработано {ru_count(len(self._results), 'покупка', 'покупки', 'покупок')}"
            )
            self._persist()
        except Exception as exc:
            self._finish_error(self._state, exc)

    def start_statistics(self, token_override: str = "") -> dict[str, Any]:
        cfg = self._config(token_override)
        with self._lock:
            if self._any_running():
                raise RuntimeError("Дождись завершения текущей операции утилиты")
            resale_ids = list(dict.fromkeys(
                str(row.get("resale_id")) for row in self._results if row.get("resale_id")
            ))
            if not resale_ids:
                raise ValueError("Сначала найди хотя бы один перепроданный аккаунт")
            self._details_cancel.clear()
            self._details_state = {
                **self._empty_operation_state(), "running": True, "status": "running",
                "started_at": utc_iso(), "total_items": len(resale_ids),
                "total_batches": math.ceil(len(resale_ids) / BULK_ITEMS_LIMIT),
                "log_start": len(self._logs),
            }
            self._details_thread = threading.Thread(
                target=self._run_statistics,
                args=(cfg, resale_ids),
                name="utility-resale-statistics",
                daemon=True,
            )
            self._details_thread.start()
        return self.status()

    def _run_statistics(self, cfg: dict[str, Any], resale_ids: list[str]) -> None:
        try:
            client, enabled_proxy = self._client(cfg)
            endpoint = f"{api_base(cfg)}/bulk/items"
            total_batches = math.ceil(len(resale_ids) / BULK_ITEMS_LIMIT)
            limiter = PostRateLimiter()
            self._log(
                f"Обновление статистики · {ru_count(len(resale_ids), 'аккаунт', 'аккаунта', 'аккаунтов')} · "
                f"{ru_count(total_batches, 'пачка', 'пачки', 'пачек')}"
            )
            for batch_index, batch in enumerate(chunks(resale_ids, BULK_ITEMS_LIMIT), start=1):
                if self._details_cancel.is_set():
                    break
                batch_label = f"Пачка {batch_index} из {total_batches}"
                self._log(
                    f"Проверка пачки {batch_index} из {total_batches} · "
                    f"{ru_count(len(batch), 'аккаунт', 'аккаунта', 'аккаунтов')}"
                )
                payload: Any = None
                item_map: dict[str, dict[str, Any]] = {}
                for payload_attempt in range(1, POST_RETRY_ATTEMPTS + 1):
                    response = self._post_batch(
                        client=client,
                        endpoint=endpoint,
                        json_body={"item_id": [int(value) for value in batch], "parse_same_item_ids": True},
                        enabled_proxy=enabled_proxy,
                        operation=self._details_state,
                        cancel_event=self._details_cancel,
                        limiter=limiter,
                        batch_label=batch_label,
                    )
                    if response is None:
                        break
                    try:
                        payload = response.json()
                    except ValueError:
                        payload = None
                    item_map = extract_bulk_item_map(payload)
                    has_status_fields = any(
                        "item_state" in item or "state" in item for item in item_map.values()
                    )
                    if item_map and has_status_fields:
                        break
                    if payload_attempt >= POST_RETRY_ATTEMPTS:
                        raise ValueError(
                            f"{batch_label}: после {POST_RETRY_ATTEMPTS} попыток API не вернул "
                            "читаемые статусы аккаунтов"
                        )
                    self._log(
                        f"{batch_label}: ответ не содержит читаемых статусов. Повтор "
                        f"{payload_attempt + 1}/{POST_RETRY_ATTEMPTS} этой же пачки через "
                        f"{POST_RETRY_WAIT_SECONDS:g} сек.",
                        "warning",
                    )
                    if self._details_cancel.wait(POST_RETRY_WAIT_SECONDS):
                        response = None
                        break
                if response is None:
                    break
                missing = 0
                with self._lock:
                    row_indexes = {
                        str(row.get("resale_id")): index
                        for index, row in enumerate(self._results) if row.get("resale_id")
                    }
                    for resale_id in batch:
                        index = row_indexes.get(str(resale_id))
                        if index is None:
                            continue
                        item = item_map.get(str(resale_id))
                        if item is None:
                            missing += 1
                            updated = dict(self._results[index])
                            updated.update({
                                "resale_state": "unknown", "resale_price": None,
                                "resale_published_date": None,
                                "resale_checked_at": utc_iso(),
                                "resale_error": "API не вернул этот ID",
                                "financial_class": "unknown", "financial_result": None,
                                "profit_percent": None,
                            })
                            self._results[index] = updated
                        else:
                            self._results[index] = enrich_financial_fields(
                                self._results[index], safe_bulk_detail_from_item(item)
                            )
                    self._details_state["processed_items"] += len(batch)
                    self._details_state["batches_done"] += 1
                    self._revision += 1
                self._log(
                    f"Пачка {batch_index} из {total_batches} готова · "
                    f"получено {len(batch) - missing} из {len(batch)}"
                )
                self._persist()
            with self._lock:
                cancelled = self._details_cancel.is_set()
                self._details_state.update({
                    "running": False, "status": "cancelled" if cancelled else "completed",
                    "finished_at": utc_iso(),
                })
            self._log("Обновление статистики остановлено", "warning") if cancelled else self._log(
                "Статистика обновлена · "
                f"{ru_count(self._details_state['processed_items'], 'аккаунт', 'аккаунта', 'аккаунтов')}"
            )
            self._persist()
        except Exception as exc:
            self._finish_error(self._details_state, exc)

    def start_tags(
        self,
        selections: list[str] | str,
        tag_ids: list[int],
        token_override: str = "",
    ) -> dict[str, Any]:
        cfg = self._config(token_override)
        allowed = {"all", "sold", "unsold", *(f"state:{state}" for state in KNOWN_ITEM_STATES), "state:unknown"}
        raw_selections = [selections] if isinstance(selections, str) else list(selections or [])
        normalized_selections = list(dict.fromkeys(str(value or "").strip() for value in raw_selections if str(value or "").strip()))
        if not normalized_selections:
            raise ValueError("Выбери хотя бы одну группу аккаунтов")
        if len(normalized_selections) > 16 or any(selection not in allowed for selection in normalized_selections):
            raise ValueError("Неизвестная группа аккаунтов для меток")
        if "all" in normalized_selections:
            normalized_selections = ["all"]
        normalized_tags = list(dict.fromkeys(int(value) for value in tag_ids if int(value) > 0))
        if not normalized_tags:
            raise ValueError("Укажи хотя бы один корректный ID метки")
        if len(normalized_tags) > 100:
            raise ValueError("За один запуск можно добавить не больше 100 меток")
        with self._lock:
            if self._any_running():
                raise RuntimeError("Дождись завершения текущей операции утилиты")
            item_ids = selected_purchase_ids(self._results, normalized_selections)
            if not item_ids:
                raise ValueError("В выбранной группе нет аккаунтов")
            self._tags_cancel.clear()
            self._tags_state = {
                **self._empty_operation_state(), "running": True, "status": "running",
                "started_at": utc_iso(), "total_items": len(item_ids),
                "total_batches": math.ceil(len(item_ids) / BULK_TAGS_LIMIT),
                "log_start": len(self._logs),
                "selections": normalized_selections, "tag_ids": normalized_tags,
            }
            self._tags_thread = threading.Thread(
                target=self._run_tags,
                args=(cfg, item_ids, normalized_tags),
                name="utility-resale-tags",
                daemon=True,
            )
            self._tags_thread.start()
        return self.status()

    def _run_tags(self, cfg: dict[str, Any], item_ids: list[int], tag_ids: list[int]) -> None:
        try:
            client, enabled_proxy = self._client(cfg)
            endpoint = f"{api_base(cfg)}/items/bulk-action"
            total_batches = math.ceil(len(item_ids) / BULK_TAGS_LIMIT)
            limiter = PostRateLimiter()
            self._log(
                f"Проставление меток · {ru_count(len(item_ids), 'аккаунт', 'аккаунта', 'аккаунтов')} · "
                f"{ru_count(total_batches, 'пачка', 'пачки', 'пачек')} · "
                f"метки: {', '.join(map(str, tag_ids))}"
            )
            for batch_index, batch in enumerate(chunks(item_ids, BULK_TAGS_LIMIT), start=1):
                if self._tags_cancel.is_set():
                    break
                batch_label = f"Пачка меток {batch_index} из {total_batches}"
                self._log(
                    f"Отправка пачки меток {batch_index} из {total_batches} · "
                    f"{ru_count(len(batch), 'аккаунт', 'аккаунта', 'аккаунтов')}"
                )
                response = self._post_batch(
                    client=client,
                    endpoint=endpoint,
                    json_body={
                        "item_ids": batch, "action": "edit-tags",
                        "add_tags": tag_ids, "remove_tags": [],
                    },
                    enabled_proxy=enabled_proxy,
                    operation=self._tags_state,
                    cancel_event=self._tags_cancel,
                    limiter=limiter,
                    batch_label=batch_label,
                )
                if response is None:
                    break
                with self._lock:
                    self._tags_state["processed_items"] += len(batch)
                    self._tags_state["batches_done"] += 1
                self._log(
                    f"Пачка меток {batch_index} из {total_batches} готова · обработано "
                    f"{ru_count(len(batch), 'аккаунт', 'аккаунта', 'аккаунтов')}"
                )
            with self._lock:
                cancelled = self._tags_cancel.is_set()
                self._tags_state.update({
                    "running": False, "status": "cancelled" if cancelled else "completed",
                    "finished_at": utc_iso(),
                })
            self._log("Проставление меток остановлено", "warning") if cancelled else self._log(
                "Метки добавлены · обработано "
                f"{ru_count(self._tags_state['processed_items'], 'аккаунт', 'аккаунта', 'аккаунтов')}"
            )
        except Exception as exc:
            self._finish_error(self._tags_state, exc)

    def _finish_error(self, operation: dict[str, Any], exc: Exception) -> None:
        message = str(exc) or type(exc).__name__
        with self._lock:
            operation.update({
                "running": False, "status": "error", "error": message,
                "current_page": None, "finished_at": utc_iso(),
            })
        self._log(message, "error")
        self._persist()

    def cancel(self) -> dict[str, Any]:
        with self._lock:
            running = bool(self._state["running"])
        if running:
            self._cancel.set()
            self._log("Получена команда остановки; завершаем текущий запрос…", "warning")
        return self.status()

    def cancel_statistics(self) -> dict[str, Any]:
        if self._details_state.get("running"):
            self._details_cancel.set()
            self._log("Останавливаем статистику после текущего пакета…", "warning")
        return self.status()

    def cancel_tags(self) -> dict[str, Any]:
        if self._tags_state.get("running"):
            self._tags_cancel.set()
            self._log("Останавливаем метки после текущего пакета…", "warning")
        return self.status()

    def clear_session(self) -> dict[str, Any]:
        with self._lock:
            if self._any_running():
                raise RuntimeError("Сначала останови текущую операцию утилиты")
            self._results = []
            self._logs.clear()
            self._revision += 1
            self._state = {
                "running": False, "status": "idle", "error": None,
                "started_at": None, "finished_at": None,
                "page_from": 1, "page_to": 3, "current_page": None,
                "pages_done": 0, "total_pages": 0, "api_requests": 0,
            }
            self._details_state = self._empty_operation_state()
            self._tags_state = self._empty_operation_state()
        try:
            self._state_path.unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise RuntimeError(f"Не удалось удалить файл сессии: {exc}") from exc
        return self.status()

    def stop(self) -> None:
        self._cancel.set()
        self._details_cancel.set()
        self._tags_cancel.set()
        for thread in (self._thread, self._details_thread, self._tags_thread):
            if thread and thread.is_alive():
                thread.join(timeout=5)

    def status(self) -> dict[str, Any]:
        with self._lock:
            rows = list(self._results)
            sold = sum(1 for row in rows if row.get("sold"))
            unsold_value = sum(
                float(row["purchase_price"])
                for row in rows
                if not row.get("sold") and isinstance(row.get("purchase_price"), (int, float))
            )
            state = dict(self._state)
            state.update({
                "ok": True, "revision": self._revision,
                "logs": list(self._logs),
                "operations": {
                    "statistics": dict(self._details_state),
                    "tags": dict(self._tags_state),
                },
                "summary": {
                    "total": len(rows), "sold": sold, "unsold": len(rows) - sold,
                    "unsold_purchase_value": round(unsold_value, 2),
                    **financial_summary(rows),
                },
            })
            return state

    def results(self) -> dict[str, Any]:
        with self._lock:
            return {"ok": True, "revision": self._revision, "results": list(self._results)}
