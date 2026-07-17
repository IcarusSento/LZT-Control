from __future__ import annotations

import json
from typing import Any

import requests

from .lzt_common import api_base
from .throttled_client import ThrottledClient


def fetch_market_item(client: ThrottledClient, config: dict, item_id: str) -> dict[str, Any] | None:
    base = api_base(config)
    r = client.get(f"{base}/{item_id}")
    if r.status_code != 200:
        if r.status_code in {408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}:
            raise requests.ConnectionError(
                f"LZT API временно недоступен (HTTP {r.status_code}) после повторов"
            )
        if r.status_code in {401, 403}:
            raise RuntimeError(f"LZT API отклонил токен или его права (HTTP {r.status_code})")
        return None
    try:
        data = r.json()
    except (json.JSONDecodeError, ValueError) as exc:
        raise requests.ConnectionError("LZT API вернул некорректный JSON") from exc
    item = data.get("item")
    if not isinstance(item, dict):
        raise requests.ConnectionError("LZT API вернул неполный ответ без item")
    return item


def check_item_alive(
    client: ThrottledClient,
    config: dict,
    item_id: str,
) -> tuple[str, str | dict]:
    """
    Быстрая проверка текущего состояния лота перед валидом / проливом.

    Возвращает (state, description):
      "ok"     — лот активен
      "resold" — ты сам отменил гарантию и перепродал; description — dict с new_item_id
      "deleted"— удалён автором или администрацией
      "error"  — не удалось проверить

    Использует ?parse_same_item_ids=true для обнаружения перепродажи:
    если в sameItemsIds есть ID > текущего, значит создан новый лот после перепродажи.
    """
    base = api_base(config)
    try:
        r = client.get(f"{base}/{item_id}?parse_same_item_ids=true")
    except Exception as e:
        return "error", str(e)

    try:
        data = r.json()
    except Exception:
        return "error", f"HTTP {r.status_code}, не JSON"

    # Лот удалён — ответ содержит список errors
    errors = data.get("errors") or []
    if isinstance(errors, list) and errors:
        first = str(errors[0]).lower()
        if "удален" in first or "удалён" in first or "deleted" in first:
            return "deleted", str(errors[0])

    if r.status_code == 404:
        return "deleted", "HTTP 404 — лот не найден"

    if r.status_code != 200:
        return "error", f"HTTP {r.status_code}"

    item = data.get("item")
    if not isinstance(item, dict):
        return "error", "нет поля item в ответе"

    # Обнаружение перепродажи: guarantee.cancelledReason == "resell"
    # + в sameItemsIds есть ID строго больше текущего
    guarantee = item.get("guarantee") or {}
    cancelled_reason = guarantee.get("cancelledReason", "")
    same_ids = data.get("sameItemsIds") or []

    if cancelled_reason == "resell" and isinstance(same_ids, list) and len(same_ids) >= 2:
        try:
            cur = int(item_id)
            new_ids = [int(x) for x in same_ids if int(x) > cur]
        except (TypeError, ValueError):
            new_ids = []
        if new_ids:
            new_id = str(max(new_ids))
            return "resold", {"new_item_id": new_id, "same_ids": same_ids}

    return "ok", str(item.get("item_state", ""))


def fetch_proliv_source(
    client: ThrottledClient,
    config: dict,
    item_id: str,
) -> tuple[str, str | dict, dict[str, Any] | None]:
    """Fetch state, Same IDs and credentials for a resale in one GET request."""
    base = api_base(config)
    try:
        response = client.get(f"{base}/{item_id}?parse_same_item_ids=true")
    except Exception as exc:
        return "error", str(exc), None
    try:
        data = response.json()
    except Exception:
        return "error", f"HTTP {response.status_code}, не JSON", None
    if not isinstance(data, dict):
        return "error", "LZT API вернул неожиданный формат ответа", None

    errors = data.get("errors") or []
    if isinstance(errors, list) and errors:
        first = str(errors[0])
        lowered = first.casefold()
        if "удален" in lowered or "удалён" in lowered or "deleted" in lowered:
            return "deleted", first, None
    if response.status_code == 404:
        return "deleted", "HTTP 404 — лот не найден", None
    if response.status_code != 200:
        return "error", str(errors or f"HTTP {response.status_code}"), None

    item = data.get("item")
    if not isinstance(item, dict):
        return "error", "нет поля item в ответе", None
    guarantee = item.get("guarantee") or {}
    cancelled_reason = guarantee.get("cancelledReason", "") if isinstance(guarantee, dict) else ""
    same_ids = data.get("sameItemsIds") or []
    if cancelled_reason == "resell" and isinstance(same_ids, list):
        try:
            current = int(item_id)
            newer_ids = [int(value) for value in same_ids if int(value) > current]
        except (TypeError, ValueError):
            newer_ids = []
        if newer_ids:
            return "resold", {"new_item_id": str(max(newer_ids)), "same_ids": same_ids}, item
    return "ok", str(item.get("item_state", "")), item


def steam_profile_url_from_item(item: dict[str, Any]) -> str | None:
    for link in item.get("accountLinks") or []:
        if not isinstance(link, dict):
            continue
        href = (link.get("link") or "").strip()
        if link.get("iconClass") == "steam" and href:
            return href
        if "steamcommunity.com" in href:
            return href
    return None


def guarantee_end_unix(item: dict[str, Any]) -> int:
    g = item.get("guarantee") or {}
    if not isinstance(g, dict):
        return 0
    try:
        return int(g.get("endDate") or 0)
    except (TypeError, ValueError):
        return 0


def extract_item_credentials(item: dict[str, Any]) -> tuple[str, str]:
    """Логин/пароль с лота (после покупки часто в loginData)."""
    ld = item.get("loginData") or item.get("login_data")
    if isinstance(ld, dict):
        lo = str(ld.get("login") or "").strip()
        pw = str(ld.get("password") or "").strip()
        if lo and pw:
            return lo, pw
    lo = str(item.get("login") or "").strip()
    pw = str(item.get("password") or "").strip()
    return lo, pw


def extract_login_password_string(item: dict[str, Any] | None) -> str:
    """
    Строка для параметра login_password в goods/check (как в рабочем скрипте: loginData.raw).
    Иначе login:password из loginData или полей лота.
    """
    if not item:
        return ""
    ld = item.get("loginData") or item.get("login_data")
    if isinstance(ld, dict):
        raw = ld.get("raw")
        if raw is not None and str(raw).strip():
            return str(raw).strip()
        lo = str(ld.get("login") or "").strip()
        pw = str(ld.get("password") or "").strip()
        if lo and pw:
            return f"{lo}:{pw}"
    lo, pw = extract_item_credentials(item)
    if lo and pw:
        return f"{lo}:{pw}"
    lp = item.get("login_password")
    if lp is not None and str(lp).strip():
        return str(lp).strip()
    return ""


def build_goods_check_body(item: dict[str, Any] | None, resell_source_item_id: str) -> dict[str, Any]:
    """Fallback: JSON-тело goods/check (если query-параметры не подошли)."""
    rid = int(resell_source_item_id)
    body: dict[str, Any] = {"resell_item_id": rid}
    if not item:
        return body
    lp = extract_login_password_string(item)
    if lp:
        body["login_password"] = lp
        return body
    login, password = extract_item_credentials(item)
    if login and password:
        body["login"] = login
        body["password"] = password
    return body
