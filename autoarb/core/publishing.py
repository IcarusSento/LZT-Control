"""LZT resale publishing.

The active pipeline uses the official one-step ``POST /item/fast-sell`` method.
Legacy item/add and goods/check helpers remain isolated below for compatibility
with old diagnostics, but are no longer used by the scheduler.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any

from .lzt_common import api_base
from .lzt_item import build_goods_check_body, check_item_alive, extract_login_password_string
from .proliv_options import normalize_extra_games
from .throttled_client import ThrottledClient, mark_lzt_retry_request


logger = logging.getLogger("lzt_control.proliv")
FAST_SELL_MAX_ATTEMPTS = 100


def _fast_sell_item_id(data: dict[str, Any]) -> int | None:
    containers: list[Any] = [data.get("item"), data.get("new_item"), data]
    items = data.get("items")
    if isinstance(items, list) and items:
        containers.append(items[0])
    for container in containers:
        if not isinstance(container, dict):
            continue
        for key in ("item_id", "itemId", "new_item_id", "id"):
            try:
                value = int(container.get(key))
            except (TypeError, ValueError):
                continue
            if value > 0:
                return value
    return None


def _fast_sell_error(data: dict[str, Any], status_code: int) -> str:
    value = data.get("errors") or data.get("message") or data.get("error")
    if value:
        body = json.dumps(data, ensure_ascii=False, separators=(",", ":"))[:3000]
        return f"{str(value)[:800]}. Ответ API: {body}"
    return f"HTTP {status_code}: API не подтвердил быстрый пролив"


def fast_sell_publish(
    client: ThrottledClient,
    config: dict,
    source_item_id: str,
    source_item: dict[str, Any],
    on_attempt: Callable[[int, int, str], None] | None = None,
) -> tuple[bool, str, int | None]:
    """Publish a resale through the official one-step fast-sell endpoint.

    Only the explicit ``retry_request`` business response is repeated. Network
    failures are ambiguous for a mutating POST and are reconciled through the
    source item's Same IDs instead of blindly creating another listing.
    """
    login_password = extract_login_password_string(source_item)
    if not login_password:
        return False, "в ответе исходного лота отсутствует login_password", None
    try:
        category_id = int(source_item.get("category_id") or config.get("proliv_default_category_id", 1))
    except (TypeError, ValueError):
        category_id = 1
    try:
        price = float(config.get("proliv_list_price_rub", 1_000_000))
    except (TypeError, ValueError):
        price = 1_000_000.0
    if price < 1:
        price = 1.0
    try:
        guarantee_duration = int(config.get("proliv_guarantee_duration", 86400))
    except (TypeError, ValueError):
        guarantee_duration = 86400
    if guarantee_duration not in {0, 43200, 86400, 259200}:
        guarantee_duration = 86400
    try:
        max_attempts = int(config.get("proliv_fast_sell_max_attempts", FAST_SELL_MAX_ATTEMPTS))
    except (TypeError, ValueError):
        max_attempts = FAST_SELL_MAX_ATTEMPTS
    max_attempts = max(1, min(FAST_SELL_MAX_ATTEMPTS, max_attempts))

    payload: dict[str, Any] = {
        "category_id": category_id,
        "currency": str(source_item.get("currency") or config.get("proliv_currency", "rub")).lower(),
        "item_origin": "resale",
        "title": str(config.get("proliv_list_title", "666")).strip()[:500] or "666",
        "price": int(price) if price.is_integer() else price,
        "guarantee_duration": guarantee_duration,
        "resell_item_id": int(source_item_id),
        "login_password": login_password,
    }
    title_en = str(config.get("proliv_list_title_en") or "").strip()
    if title_en:
        payload["title_en"] = title_en[:500]
    extra_games = normalize_extra_games(config.get("proliv_extra_games"))
    if extra_games:
        payload["extra"] = extra_games

    # This endpoint is documented on prod-api and is intentionally not sent to
    # the legacy api.lzt.market alias that older configurations may contain.
    url = "https://prod-api.lzt.market/item/fast-sell"
    last_retry_response = ""
    for attempt in range(1, max_attempts + 1):
        if on_attempt:
            on_attempt(attempt, max_attempts, "request")
        try:
            response = client.post(url, json_body=payload)
        except InterruptedError:
            raise
        except Exception as exc:
            # A write may have reached LZT even when its response was lost.
            state, description = check_item_alive(client, config, source_item_id)
            if state == "resold" and isinstance(description, dict):
                try:
                    new_id = int(description.get("new_item_id"))
                except (TypeError, ValueError):
                    new_id = None
                return True, f"fast-sell подтверждён через Same IDs после потери ответа; попытка {attempt}", new_id
            return False, f"uncertain: соединение оборвалось во время fast-sell ({exc})", None
        try:
            data = response.json()
        except (ValueError, TypeError):
            return False, f"uncertain: fast-sell вернул не JSON (HTTP {response.status_code})", None
        if not isinstance(data, dict):
            return False, "uncertain: fast-sell вернул неожиданный формат ответа", None

        if _response_suggests_retry(data):
            last_retry_response = json.dumps(
                data, ensure_ascii=False, separators=(",", ":")
            )[:3000]
            mark_lzt_retry_request(response, attempt, max_attempts)
            if on_attempt:
                on_attempt(attempt, max_attempts, "retry_request")
            if attempt < max_attempts:
                if attempt == 1 or attempt % 10 == 0:
                    logger.info(
                        "[Proliv] fast-sell #%s: retry_request, попытка %d/%d",
                        source_item_id, attempt, max_attempts,
                    )
                continue
            return False, (
                f"retry_request: исчерпаны официальные {max_attempts} попыток fast-sell. "
                f"Последний ответ API: {last_retry_response}"
            ), None

        new_id = _fast_sell_item_id(data)
        success = response.status_code == 200 and (
            str(data.get("status") or "").casefold() == "ok" or new_id is not None
        )
        if success:
            if new_id is None:
                state, description = check_item_alive(client, config, source_item_id)
                if state == "resold" and isinstance(description, dict):
                    try:
                        new_id = int(description.get("new_item_id"))
                    except (TypeError, ValueError):
                        new_id = None
            return True, f"ok_fast_sell_attempt_{attempt}", new_id
        return False, _fast_sell_error(data, response.status_code), None
    return False, (
        f"retry_request: исчерпаны официальные {max_attempts} попыток fast-sell. "
        f"Последний ответ API: {last_retry_response}"
    ), None


def item_add_resell(
    client: ThrottledClient,
    config: dict,
    source_item: dict[str, Any],
    source_item_id: str,
) -> tuple[int | None, str]:
    """POST /item/add — параметры в query (как в присланном скрипте)."""
    base = api_base(config)
    title = str(config.get("proliv_list_title", "666")).strip()[:500] or "666"
    try:
        price = float(config.get("proliv_list_price_rub", 1_000_000))
    except (TypeError, ValueError):
        price = 1_000_000.0
    if price <= 0:
        price = float(config.get("proliv_list_price_rub", 1_000_000))
    try:
        cat = int(source_item.get("category_id") or config.get("proliv_default_category_id", 1))
    except (TypeError, ValueError):
        cat = 1
    currency = str(source_item.get("currency") or config.get("proliv_currency", "rub")).lower()
    try:
        ext_g = source_item.get("extended_guarantee")
        if ext_g is None:
            ext_g = int(config.get("proliv_extended_guarantee", 0))
        else:
            ext_g = int(ext_g)
    except (TypeError, ValueError):
        ext_g = 0

    params: dict[str, str] = {
        "title": title,
        "price": str(int(price)) if price == int(price) else str(price),
        "category_id": str(cat),
        "currency": currency,
        "item_origin": "resale",
        "extended_guarantee": str(ext_g),
    }
    if config.get("proliv_resell_id_in_item_add", False):
        params["resell_item_id"] = str(int(source_item_id))
    te = config.get("proliv_list_title_en")
    if te:
        params["title_en"] = str(te).strip()[:500]
    elif config.get("proliv_copy_title_en_from_source"):
        te_src = source_item.get("title_en")
        if te_src:
            params["title_en"] = str(te_src)[:500]

    r = client.post(f"{base}/item/add", params=params)
    try:
        data = r.json()
    except (ValueError, TypeError):
        return None, "uncertain: LZT API не вернул JSON после item/add"
    if not isinstance(data, dict):
        if r.status_code == 200:
            return None, "uncertain: LZT API returned an unexpected item/add response"
        return None, f"item/add: HTTP {r.status_code}"
    if r.status_code != 200:
        return None, str(data.get("errors") or data)[:500]
    if data.get("status") != "ok":
        return None, str(data.get("errors") or data.get("message") or data)[:500]
    it = data.get("item") or {}
    if not isinstance(it, dict):
        return None, "uncertain: no item in successful item/add response"
    new_id = it.get("item_id") or it.get("itemId")
    if new_id is None:
        return None, "uncertain: successful item/add response has no item_id"
    try:
        return int(new_id), "ok"
    except (TypeError, ValueError):
        return None, "uncertain: successful item/add response has invalid item_id"


def _response_suggests_retry(data: dict[str, Any]) -> bool:
    errs = data.get("errors")
    blob = json.dumps(data, ensure_ascii=False) if data else ""
    if "retry_request" in blob:
        return True
    if isinstance(errs, list) and any("retry_request" in str(e) for e in errs):
        return True
    if isinstance(errs, dict):
        return "retry_request" in json.dumps(errs, ensure_ascii=False)
    return False


def goods_check_publish(
    client: ThrottledClient,
    config: dict,
    draft_item_id: str,
    resell_source_item_id: str,
    source_item: dict[str, Any] | None,
) -> tuple[bool, str]:
    """
    POST /{draft}/goods/check: сначала query (login_password + resell_item_id + close_item),
    при неудаче без retry_request — вторая фаза JSON-тело.
    """
    base = api_base(config)
    url = f"{base}/{draft_item_id}/goods/check"
    lp = extract_login_password_string(source_item) if source_item else ""

    # extra->games: {"uplay_games": true, "ea_games": true, ...} из конфига
    extra_games = normalize_extra_games(config.get("proliv_extra_games"))

    phases: list[str] = []
    if config.get("proliv_goods_check_use_query", True) and lp:
        phases.append("query")
    phases.append("json")

    last = ""
    request_attempts = 0
    for mode in phases:
        if mode == "query" and not lp:
            continue
        while request_attempts < 3:
            request_attempts += 1
            if mode == "query":
                qp: dict[str, str] = {
                    "login_password": lp,
                    "resell_item_id": str(int(resell_source_item_id)),
                }
                if config.get("proliv_goods_check_close_item", True):
                    qp["close_item"] = "true"
                # extra передаём как JSON-тело рядом с query-параметрами
                extra_body = {"extra": extra_games} if extra_games else None
                r = client.post(url, params=qp, json_body=extra_body)
            else:
                body = build_goods_check_body(source_item, resell_source_item_id)
                if extra_games:
                    body["extra"] = extra_games
                r = client.post(url, json_body=body if body else None)

            last = f"HTTP {r.status_code}"
            try:
                data = r.json()
            except (ValueError, TypeError):
                last = (r.text or "")[:300]
                break
            if not isinstance(data, dict):
                last = f"API returned an unexpected response ({mode})"
                break
            if r.status_code in {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}:
                body = json.dumps(data, ensure_ascii=False, separators=(",", ":"))[:1200]
                return False, f"LZT API временно недоступен (HTTP {r.status_code}): {body}"
            if _response_suggests_retry(data):
                body = json.dumps(data, ensure_ascii=False, separators=(",", ":"))[:1200]
                if request_attempts < 3:
                    # ThrottledClient already applies the configured pause.
                    # Ask for the completed asynchronous result up to three
                    # times before returning control to the scheduler.
                    continue
                return False, (
                    "LZT ещё обрабатывает проверку аккаунта и попросил повторить запрос позже "
                    f"(retry_request). Ответ API: {body}"
                )
            if r.status_code != 200:
                last = str(data.get("errors") or data)[:500]
                break
            if data.get("status") == "ok":
                return True, f"ok_{mode}"
            if data.get("item"):
                return True, f"ok_item_{mode}"
            err = data.get("errors")
            if err:
                # Это бизнес-ответ goods/check, а не ошибка формата запроса.
                # Повтор тем же запуском способен лишь продублировать проверку;
                # решение о паузе принимает единая очередь пролива.
                return False, json.dumps(data, ensure_ascii=False, separators=(",", ":"))[:1200]
            last = f"API не подтвердил публикацию ({mode}): нет status или item"
            break
    return False, last or "retry_limit"


def post_item_tag(
    client: ThrottledClient,
    config: dict,
    item_id: str,
    tag_id: int,
) -> tuple[bool, str]:
    """POST /{item_id}/tag — JSON {\"tag_id\": ...} (как prod-api.lzt.market)."""
    try:
        iid = int(item_id)
        tid = int(tag_id)
    except (TypeError, ValueError):
        return False, "некорректный item_id или tag_id"
    if tid <= 0:
        return False, "tag_id не задан"
    base = api_base(config)
    r = client.post(f"{base}/{iid}/tag", json_body={"tag_id": tid})
    try:
        data = r.json()
    except json.JSONDecodeError:
        return False, (r.text or "")[:300] or f"HTTP {r.status_code}"
    if r.status_code == 200 and data.get("status") == "ok":
        return True, "ok"
    return False, str(data.get("errors") or data.get("message") or data)[:500]
