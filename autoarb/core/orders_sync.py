"""Загрузка покупок по ссылке из конфига, расчёт расписания 4 проверок (валид + КТ)."""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlencode, urlparse

from rich.console import Console

from .lzt_common import api_base
from .paths import DATES_OF_CHECK, GUARANTEE_TXT
from .storage import (
    dates_file_lock,
    load_checked_items,
    reference_file_lock,
    save_checked_item,
    sort_dates_file,
)
from .throttled_client import ThrottledClient

console = Console()


def _api_error(context: str, response) -> RuntimeError:
    detail = ""
    try:
        payload = response.json()
        if isinstance(payload, dict):
            detail = str(payload.get("error") or payload.get("message") or payload.get("errors") or "")
    except Exception:
        detail = str(getattr(response, "text", "") or "")[:240]
    code = int(response.status_code)
    if code == 401:
        message = "API-токен LZT недействителен или истёк"
    elif code == 403:
        message = "LZT API запретил запрос — проверь права токена"
    elif code == 429:
        message = "LZT API ограничил частоту запросов"
    elif 500 <= code <= 599:
        message = "LZT Market временно недоступен или находится на техработах"
    else:
        message = f"LZT API вернул HTTP {code}"
    return RuntimeError(f"{context}: {message}" + (f" ({detail})" if detail else ""))


def _emit_sync(ui_state: dict | None, markup_line: str) -> None:
    """В main.py лог идёт в панель; иначе — обычный print (не смешивать с Rich Live)."""
    if ui_state is not None:
        logs = ui_state.setdefault("sync_log", [])
        logs.append(markup_line)
        if len(logs) > 16:
            del logs[:-16]
    else:
        console.print(markup_line)


def build_orders_api_url(config: dict, link: str | None, page: int) -> str:
    """
    Список покупок — см. API «Get All Purchased Accounts»: GET /user/orders
    (https://lzt-market.readme.io/reference/listorders).
    Пользователь задаётся токеном; query-параметры берём из link (как в браузере).
    """
    root = api_base(config)
    if config.get("orders_list_use_path_user_id", False):
        uid = str(config.get("user_id") or "").strip()
        base = f"{root}/user/{uid}/orders"
    else:
        base = f"{root}/user/orders"
    params: dict[str, list[str]] = {}
    link_has_query = bool(link and urlparse(link).query)
    if link:
        q = urlparse(link).query
        if q:
            params = parse_qs(q, keep_blank_values=True)
    params["page"] = [str(page)]
    # Ранняя остановка по страницам без гарантии имеет смысл только при
    # стабильной сортировке от новых покупок к старым. Фильтры пользователя
    # сохраняем, но порядок всегда нормализуем.
    params["order_by"] = ["pdate_to_down"]
    # Если в link нет своих фильтров — как раньше по умолчанию Steam (1)
    if not link_has_query and "category_id" not in params:
        params.setdefault("category_id", ["1"])
    query = urlencode(params, doseq=True)
    return f"{base}?{query}"


def active_guarantee_from_order(item: dict, *, now_unix: int | None = None) -> dict | None:
    """Return an active guarantee embedded in a /user/orders item.

    The orders endpoint already returns complete item objects.  Polling must
    not issue an additional GET /{item_id} merely to read guarantee.endDate.
    """
    guarantee = item.get("guarantee")
    if not isinstance(guarantee, dict) or not guarantee:
        return None
    if "active" in guarantee and not bool(guarantee.get("active")):
        return None
    if bool(guarantee.get("cancelled")):
        return None
    if str(guarantee.get("cancelledReason") or "").strip():
        return None
    try:
        end_unix = int(guarantee.get("endDate") or 0)
    except (TypeError, ValueError):
        return None
    if end_unix <= (int(time.time()) if now_unix is None else int(now_unix)):
        return None
    return guarantee


def _no_guarantee_page_limit(config: dict) -> int:
    try:
        value = int(config.get("orders_no_guarantee_page_limit", 2))
    except (TypeError, ValueError):
        value = 2
    return max(1, min(3, value))


def fetch_order_items(
    client: ThrottledClient,
    config: dict,
    link: str | None,
    *,
    ui_state: dict | None = None,
) -> list[dict]:
    """Load only purchased accounts whose embedded guarantee is active.

    Pages are scanned in newest-first order.  When enabled, polling stops
    after 1..3 consecutive non-empty pages without an active guarantee.
    Accounts without a guarantee are deliberately neither returned nor stored.
    """
    items_by_id: dict[str, dict] = {}
    page = 1
    pages_without_guarantee = 0
    stop_early = bool(config.get("orders_stop_without_guarantee_enabled", True))
    stop_limit = _no_guarantee_page_limit(config)
    while True:
        url = build_orders_api_url(config, link, page)
        r = client.get(url)
        if r.status_code != 200:
            _emit_sync(ui_state, f"[red]Список заказов: HTTP {r.status_code}[/red]")
            raise _api_error("Список заказов", r)
        try:
            data = r.json()
        except (json.JSONDecodeError, ValueError) as exc:
            raise RuntimeError("Список заказов: LZT API вернул некорректный JSON") from exc
        items = data.get("items")
        if not isinstance(items, list):
            raise RuntimeError("Список заказов: в ответе LZT API отсутствует массив items")
        if not items:
            _emit_sync(ui_state, f"[dim]Страница {page}: покупок нет — конец списка[/dim]")
            break

        page_active = 0
        for it in items:
            if not isinstance(it, dict) or "item_id" not in it:
                continue
            if active_guarantee_from_order(it) is None:
                continue
            item_id = str(it["item_id"])
            if item_id:
                items_by_id.setdefault(item_id, it)
                page_active += 1

        if page_active:
            pages_without_guarantee = 0
            _emit_sync(
                ui_state,
                f"[green]Страница {page}: на гарантии {page_active} из {len(items)}[/green]",
            )
        else:
            pages_without_guarantee += 1
            _emit_sync(
                ui_state,
                f"[dim]Страница {page}: активных гарантий нет "
                f"({pages_without_guarantee}/{stop_limit})[/dim]",
            )

        if stop_early and pages_without_guarantee >= stop_limit:
            _emit_sync(
                ui_state,
                f"[yellow]Опрос остановлен: {pages_without_guarantee} стр. подряд без гарантии[/yellow]",
            )
            break
        if len(items) < 40:
            break
        page += 1
        if page > 250:
            raise RuntimeError("Список заказов превысил безопасный предел в 250 страниц")
    return list(items_by_id.values())


def fetch_order_item_ids(
    client: ThrottledClient,
    config: dict,
    link: str | None,
    *,
    ui_state: dict | None = None,
) -> list[str]:
    """Backward-compatible ID view for diagnostics and older integrations."""
    return [str(item["item_id"]) for item in fetch_order_items(
        client, config, link, ui_state=ui_state,
    )]


def slot_label(slot: str) -> str:
    """Человекочитаемое название слота для логов и TUI.
    P10 → '10%', P95 → '95%', устаревшие S/M/M2/E → старые названия.
    """
    if slot.startswith("P") and slot[1:].isdigit():
        return f"{slot[1:]}%"
    return {
        "S": "сразу", "M": "середина", "M2": "середина+2ч", "E": "финал",
        "manual": "вручную", "recovery": "последняя",
    }.get(slot, slot)


def build_check_schedule(
    guarantee_info: dict,
    *,
    percents: list[int] | None = None,
) -> tuple[str, list[tuple[str, str]]]:
    """
    Возвращает (статус_строка, [(slot, date_str), ...]).

    Для каждого процента из percents (по умолчанию [10, 55, 99]) вычисляет
    момент проверки как: now + total_duration * pct / 100.
    Слот называется 'P{pct}' (напр. 'P10', 'P95').

    Точки в прошлом, за пределами гарантии и ближе 5 мин друг к другу пропускаются.
    """
    if percents is None:
        percents = [10, 55, 99]

    end_date_unix = guarantee_info.get("endDate", 0)
    if not end_date_unix:
        return "Гарантия истекла", []

    end_dt = datetime.fromtimestamp(int(end_date_unix), tz=timezone.utc) + timedelta(hours=3)
    now = datetime.now(timezone.utc) + timedelta(hours=3)

    if end_dt <= now:
        return "Гарантия истекла", []

    status = "Гарантия истекает: " + end_dt.strftime("%d-%m-%Y %H:%M:%S")
    total_sec = (end_dt - now).total_seconds()

    MIN_GAP = timedelta(minutes=5)
    schedule: list[tuple[str, datetime]] = []

    for pct in sorted(set(percents)):
        if not (1 <= pct <= 99):
            continue
        dt = now + timedelta(seconds=total_sec * pct / 100)
        if dt <= now or dt >= end_dt:
            continue
        if schedule and (dt - schedule[-1][1]) < MIN_GAP:
            continue
        schedule.append((f"P{pct}", dt))

    result = [(s, dt.strftime("%d-%m-%Y %H:%M:%S")) for s, dt in schedule]
    return status, result


def append_guarantee_line(path, item_id: str, guarantee_status: str) -> None:
    with reference_file_lock:
        with path.open("a", encoding="utf-8") as f:
            f.write(f"Item ID: {item_id}, Статус гарантии: {guarantee_status}\n")


def append_scheduled_check(item_id: str, slot: str, check_date: str) -> None:
    """Записывает одну точку проверки. Ключ в файле: item_id#slot (напр. 123456#P30)."""
    key = f"{item_id}#{slot}"
    with dates_file_lock:
        with DATES_OF_CHECK.open("a", encoding="utf-8") as f:
            f.write(f"Дата проверки для {key}: {check_date}\n")


def run_sync_cycle(
    client: ThrottledClient,
    config: dict,
    checked_path,
    *,
    ui_state: dict | None = None,
) -> tuple[int, int]:
    """Обрабатывает новые гарантии. Возвращает (новых, активных найдено)."""
    if ui_state is not None:
        ui_state["sync_log"] = []
    link = config.get("link")
    checked = load_checked_items(checked_path)
    guaranteed_items = fetch_order_items(client, config, link, ui_state=ui_state)
    new_items = [
        item for item in guaranteed_items
        if str(item.get("item_id") or "") not in checked
    ]
    try:
        percents_raw = config.get("check_schedule_percents", [10, 55, 99])
        percents = [int(p) for p in percents_raw if 1 <= int(p) <= 99]
        if not percents:
            percents = [10, 55, 99]
    except (TypeError, ValueError):
        percents = [10, 55, 99]

    processed = 0
    for item in new_items:
        item_id = str(item.get("item_id") or "")
        g = active_guarantee_from_order(item)
        if not item_id or g is None:
            continue
        status, checks = build_check_schedule(g, percents=percents)
        # Гарантия могла закончиться, пока обрабатывались предыдущие страницы.
        # Такие аккаунты не записываем ни в одну локальную базу.
        if status == "Гарантия истекла":
            continue
        _emit_sync(ui_state, f"[green]{item_id}[/green] — {status}")
        append_guarantee_line(GUARANTEE_TXT, item_id, status)
        for s, check_date in checks:
            append_scheduled_check(item_id, s, check_date)
        if checks:
            labels = ", ".join(slot_label(s) for s, _ in checks)
            _emit_sync(ui_state, f"  [dim]→ {len(checks)} проверок: {labels}[/dim]")
        save_checked_item(checked_path, item_id)
        processed += 1
    if processed:
        sort_dates_file(DATES_OF_CHECK)
    return processed, len(guaranteed_items)
