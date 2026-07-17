from __future__ import annotations

import math
import threading
import time
from collections import deque
from datetime import datetime
from typing import Any, Callable
from urllib.parse import parse_qs, urlencode, urlsplit

from autoarb.core.lzt_common import api_base
from autoarb.core.throttled_client import ThrottledClient, use_lzt_proxy_from_config

from .resale_finder import ALLOWED_LINK_HOSTS, BULK_ITEMS_LIMIT, extract_bulk_item_map


MAX_PAGES = 100
MODES = {"reseller", "seller"}
SAME_ID_KEYS = ("sameItemsIds", "sameItemIds", "same_items_ids")


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


def _unix(value: Any) -> int | None:
    try:
        timestamp = int(float(value))
    except (TypeError, ValueError):
        return None
    if timestamp <= 0:
        return None
    if timestamp > 10_000_000_000:
        timestamp //= 1000
    return timestamp


def _same_ids(item: dict[str, Any]) -> list[int]:
    raw: Any = None
    for key in SAME_ID_KEYS:
        if key in item:
            raw = item.get(key)
            break
    result: set[int] = set()
    if isinstance(raw, list):
        for value in raw:
            try:
                item_id = int(value)
            except (TypeError, ValueError):
                continue
            if item_id > 0:
                result.add(item_id)
    return sorted(result)


def build_sales_url(source_url: str, base_url: str, page: int) -> str:
    parsed = urlsplit(str(source_url or "").strip())
    if parsed.scheme != "https" or parsed.hostname not in ALLOWED_LINK_HOSTS:
        raise ValueError("Нужна официальная HTTPS-ссылка LZT Market на список своих продаж")
    if "/items" not in parsed.path.rstrip("/"):
        raise ValueError("В ссылке не найден раздел продаж /items")
    if page < 1:
        raise ValueError("Номер страницы должен быть больше нуля")
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["page"] = [str(page)]
    params["parse_same_item_ids"] = ["true"]
    params.setdefault("show", ["active"])
    params.setdefault("order_by", ["pdate_to_down"])
    return f"{base_url}/user/items?{urlencode(params, doseq=True)}"


def _extract_items(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise ValueError("LZT API вернул некорректный JSON")
    items = payload.get("items")
    if isinstance(items, dict):
        values = list(items.values())
    elif isinstance(items, list):
        values = items
    else:
        raise ValueError("LZT API не вернул список аккаунтов items")
    result: list[dict[str, Any]] = []
    for value in values:
        if not isinstance(value, dict):
            continue
        if isinstance(value.get("item"), dict):
            item = dict(value["item"])
            for key in SAME_ID_KEYS:
                if key not in item and key in value:
                    item[key] = value.get(key)
            result.append(item)
        else:
            result.append(dict(value))
    for item in result:
        if any(key in item for key in SAME_ID_KEYS):
            continue
        raw_id = item.get("item_id") or item.get("itemId") or item.get("id")
        for mapping_key in ("sameItemsIdsByItem", "sameItemIdsByItem", "same_items_ids_by_item"):
            mapping = payload.get(mapping_key)
            if not isinstance(mapping, dict):
                continue
            relation = mapping.get(str(raw_id))
            if relation is None:
                relation = mapping.get(raw_id)
            if relation is not None:
                item["sameItemsIds"] = relation
                break
    return result


def _api_error(response: Any) -> str:
    try:
        payload = response.json()
    except ValueError:
        return str(getattr(response, "text", "") or "")[:300] or f"HTTP {response.status_code}"
    if isinstance(payload, dict):
        return str(payload.get("errors") or payload.get("error") or payload.get("message") or payload)[:500]
    return str(payload)[:500]


def _profit(value: int | float | None, cost: int | float | None) -> tuple[int | float | None, float | None]:
    if value is None or cost is None:
        return None, None
    amount = round(float(value) - float(cost), 2)
    percent = round(amount / float(cost) * 100, 2) if float(cost) > 0 else None
    return amount, percent


class DealFinderService:
    def __init__(self, config_loader: Callable[[], dict[str, Any]]) -> None:
        self._config_loader = config_loader
        self._lock = threading.RLock()
        self._cancel = threading.Event()
        self._thread: threading.Thread | None = None
        self._logs: deque[dict[str, str]] = deque(maxlen=400)
        self._results: list[dict[str, Any]] = []
        self._revision = 0
        self._state = self._empty_state()

    @staticmethod
    def _empty_state() -> dict[str, Any]:
        return {
            "running": False,
            "status": "idle",
            "error": None,
            "mode": "reseller",
            "phase": None,
            "started_at": None,
            "finished_at": None,
            "page_from": 1,
            "page_to": 1,
            "current_page": None,
            "pages_done": 0,
            "total_pages": 0,
            "processed_steps": 0,
            "total_steps": 0,
            "api_requests": 0,
        }

    def _log(self, message: str, level: str = "info") -> None:
        with self._lock:
            self._logs.append({
                "at": datetime.now().strftime("%H:%M:%S"),
                "level": level,
                "message": message,
            })

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "ok": True,
                **dict(self._state),
                "revision": self._revision,
                "result_count": len(self._results),
                "logs": list(self._logs),
                "bulk_limit": BULK_ITEMS_LIMIT,
            }

    def results(self) -> dict[str, Any]:
        with self._lock:
            return {
                "ok": True,
                "revision": self._revision,
                "mode": self._state.get("mode") or "reseller",
                "results": [dict(row) for row in self._results],
            }

    def _client(self, token: str) -> tuple[ThrottledClient, bool, dict[str, Any]]:
        cfg = dict(self._config_loader() or {})
        cfg["token"] = token
        enabled_proxy, proxy = use_lzt_proxy_from_config(cfg)
        client = ThrottledClient(
            token,
            proxy,
            delay_seconds=max(0.0, float(cfg.get("request_delay_seconds", 3) or 3)),
            use_lzt_proxy=enabled_proxy,
            source="Утилита: выгодные предложения",
        )
        return client, enabled_proxy, cfg

    def start(
        self,
        token: str,
        source_url: str,
        page_from: int,
        page_to: int,
        mode: str,
    ) -> dict[str, Any]:
        token = str(token or "").strip()
        mode = str(mode or "reseller").strip().lower()
        if not token:
            raise ValueError("Выбери LZT-аккаунт с API-токеном")
        if mode not in MODES:
            raise ValueError("Неизвестный режим анализа")
        if page_from < 1 or page_to < page_from:
            raise ValueError("Проверь диапазон страниц")
        if page_to - page_from + 1 > MAX_PAGES:
            raise ValueError(f"За один запуск можно обработать не больше {MAX_PAGES} страниц")
        cfg = dict(self._config_loader() or {})
        build_sales_url(source_url, api_base(cfg), page_from)
        with self._lock:
            if self._state.get("running"):
                raise RuntimeError("Анализ уже выполняется")
            self._cancel.clear()
            self._results = []
            self._logs.clear()
            self._revision += 1
            total_pages = page_to - page_from + 1
            self._state = {
                **self._empty_state(),
                "running": True,
                "status": "running",
                "mode": mode,
                "phase": "sales",
                "started_at": datetime.now().isoformat(timespec="seconds"),
                "page_from": page_from,
                "page_to": page_to,
                "current_page": page_from,
                "total_pages": total_pages,
                "total_steps": total_pages,
            }
            self._thread = threading.Thread(
                target=self._run,
                args=(token, source_url, page_from, page_to, mode),
                name="utility-deal-finder",
                daemon=True,
            )
            self._thread.start()
        return self.status()

    def _read_json(self, response: Any, context: str) -> Any:
        with self._lock:
            self._state["api_requests"] += 1
        if not 200 <= response.status_code < 300:
            raise ValueError(f"{context}: LZT API вернул HTTP {response.status_code} — {_api_error(response)}")
        try:
            return response.json()
        except ValueError as exc:
            raise ValueError(f"{context}: LZT API вернул повреждённый JSON") from exc

    @staticmethod
    def _sale_row(item: dict[str, Any], mode: str, now_unix: int) -> dict[str, Any] | None:
        try:
            sale_id = int(item.get("item_id") or item.get("itemId") or item.get("id") or 0)
        except (TypeError, ValueError):
            return None
        if sale_id <= 0:
            return None
        published_date = _unix(item.get("published_date"))
        auto_buy_price = _money(item.get("autoBuyPrice") if "autoBuyPrice" in item else item.get("auto_buy_price"))
        can_check = bool(item.get("canCheckAutoBuyPrice", item.get("can_check_auto_buy_price", False)))
        same_ids = _same_ids(item)
        previous = [value for value in same_ids if value < sale_id]
        previous_id = max(previous) if previous and mode == "reseller" else None
        sale_price = _money(item.get("rub_price") if item.get("rub_price") not in (None, "") else item.get("price"))
        days_on_sale = max(0, (now_unix - published_date) // 86_400) if published_date else None
        sale_gap = None
        sale_gap_percent = None
        if sale_price is not None and auto_buy_price is not None:
            sale_gap = round(float(sale_price) - float(auto_buy_price), 2)
            sale_gap_percent = round(sale_gap / float(sale_price) * 100, 2) if float(sale_price) > 0 else None
        return {
            "sale_id": sale_id,
            "sale_url": f"https://lzt.market/{sale_id}/",
            "purchase_id": previous_id,
            "purchase_url": f"https://lzt.market/{previous_id}/" if previous_id else None,
            "purchase_date": None,
            "purchase_price": None,
            "published_date": published_date,
            "days_on_sale": days_on_sale,
            "sale_price": sale_price,
            "auto_buy_price": auto_buy_price,
            "auto_buy_available": can_check and auto_buy_price is not None,
            "auto_buy_check_date": _unix(item.get("autoBuyPriceCheckDate") or item.get("auto_buy_price_check_date")),
            "regular_profit": None,
            "regular_profit_percent": None,
            "auto_profit": None,
            "auto_profit_percent": None,
            "sale_auto_gap": sale_gap,
            "sale_auto_gap_percent": sale_gap_percent,
            "error": None,
        }

    @staticmethod
    def _enrich_purchase(row: dict[str, Any], item: dict[str, Any] | None) -> dict[str, Any]:
        result = dict(row)
        if not item:
            result["error"] = "Данные покупки не найдены"
            return result
        buyer = item.get("buyer") if isinstance(item.get("buyer"), dict) else {}
        purchase_price = _money(item.get("rub_price") if item.get("rub_price") not in (None, "") else item.get("price"))
        purchase_date = _unix(buyer.get("operation_date") or item.get("operation_date") or item.get("paid_date"))
        result["purchase_price"] = purchase_price
        result["purchase_date"] = purchase_date
        result["regular_profit"], result["regular_profit_percent"] = _profit(result.get("sale_price"), purchase_price)
        result["auto_profit"], result["auto_profit_percent"] = _profit(result.get("auto_buy_price"), purchase_price)
        return result

    def _run(self, token: str, source_url: str, page_from: int, page_to: int, mode: str) -> None:
        try:
            client, enabled_proxy, cfg = self._client(token)
            base_url = api_base(cfg)
            now_unix = int(time.time())
            seen: set[int] = set()
            sales: list[dict[str, Any]] = []
            total_pages = page_to - page_from + 1
            mode_label = "реселлер" if mode == "reseller" else "продавец"
            self._log(f"Анализ начат · режим: {mode_label} · страницы {page_from}–{page_to}")
            for page in range(page_from, page_to + 1):
                if self._cancel.is_set():
                    break
                with self._lock:
                    self._state["current_page"] = page
                self._log(f"Продажи · страница {page} из {page_to}")
                response = client.get(build_sales_url(source_url, base_url, page), use_proxy=enabled_proxy)
                payload = self._read_json(response, f"Страница {page}")
                page_rows: list[dict[str, Any]] = []
                for item in _extract_items(payload):
                    row = self._sale_row(item, mode, now_unix)
                    if row and row["sale_id"] not in seen:
                        seen.add(row["sale_id"])
                        page_rows.append(row)
                sales.extend(page_rows)
                with self._lock:
                    self._state["pages_done"] += 1
                    self._state["processed_steps"] += 1
                    self._revision += 1
                self._log(f"Продажи · страница {page} готова · найдено {len(page_rows)} аккаунтов")

            if self._cancel.is_set():
                raise InterruptedError

            if mode == "reseller":
                previous_ids = list(dict.fromkeys(int(row["purchase_id"]) for row in sales if row.get("purchase_id")))
                batches = [previous_ids[index:index + BULK_ITEMS_LIMIT] for index in range(0, len(previous_ids), BULK_ITEMS_LIMIT)]
                with self._lock:
                    self._state["phase"] = "purchases"
                    self._state["total_steps"] = total_pages + len(batches)
                    self._state["current_page"] = None
                self._log(f"Покупки · {len(previous_ids)} ID · пачек {len(batches)} по {BULK_ITEMS_LIMIT}")
                purchase_items: dict[str, dict[str, Any]] = {}
                endpoint = f"{base_url}/bulk/items"
                for batch_index, batch in enumerate(batches, start=1):
                    if self._cancel.is_set():
                        raise InterruptedError
                    self._log(f"Покупки · пачка {batch_index}/{len(batches)} · {len(batch)} аккаунтов")
                    response = client.post(
                        endpoint,
                        json_body={"item_id": batch, "parse_same_item_ids": True},
                        use_proxy=enabled_proxy,
                        retry_safe=True,
                    )
                    payload = self._read_json(response, f"Пачка покупок {batch_index}")
                    purchase_items.update(extract_bulk_item_map(payload))
                    with self._lock:
                        self._state["processed_steps"] += 1
                    self._log(f"Покупки · пачка {batch_index}/{len(batches)} готова")
                sales = [self._enrich_purchase(row, purchase_items.get(str(row.get("purchase_id")))) for row in sales]

            with self._lock:
                self._results = sales
                self._revision += 1
                self._state.update({
                    "running": False,
                    "status": "completed",
                    "phase": None,
                    "current_page": None,
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                })
            with_auto = sum(row.get("auto_buy_price") is not None for row in sales)
            self._log(f"Анализ завершён · аккаунтов {len(sales)} · цен скупщиков {with_auto}")
        except InterruptedError:
            with self._lock:
                self._state.update({
                    "running": False,
                    "status": "cancelled",
                    "phase": None,
                    "current_page": None,
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                })
            self._log("Анализ остановлен", "warning")
        except Exception as exc:
            message = str(exc) or type(exc).__name__
            with self._lock:
                self._state.update({
                    "running": False,
                    "status": "error",
                    "error": message,
                    "phase": None,
                    "current_page": None,
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                })
                self._revision += 1
            self._log(message, "error")

    def cancel(self) -> dict[str, Any]:
        self._cancel.set()
        self._log("Останавливаем анализ после текущего запроса…", "warning")
        return self.status()

    def clear(self) -> dict[str, Any]:
        with self._lock:
            if self._state.get("running"):
                raise RuntimeError("Сначала останови анализ")
            self._results = []
            self._logs.clear()
            self._revision += 1
            self._state = self._empty_state()
        return self.status()

    def stop(self) -> None:
        self._cancel.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=5)
