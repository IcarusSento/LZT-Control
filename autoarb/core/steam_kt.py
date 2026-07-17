"""
Проверка «КТ» через Steam Web API (логика из Main3.py).
Ключ: https://steamcommunity.com/dev/apikey — задаётся в config.json (steam_web_api_key).
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any

import requests

VALID_VISIBILITY = 3
MAX_REQUEST_ATTEMPTS = 3
RETRYABLE_HTTP_CODES = {408, 425, 429, 500, 502, 503, 504}

_session = requests.Session()
_session.headers.update({"User-Agent": "LZTAutoCheck-KT/1.0"})
logger = logging.getLogger("lzt_control.steam")


def extract_steamid64(steam_url: str, api_key: str) -> str | None:
    m = re.search(r"/profiles/(\d{15,20})", steam_url)
    if m:
        return m.group(1)
    m = re.search(r"/id/([^/?\s]+)", steam_url)
    if m:
        return resolve_vanity(m.group(1), api_key)
    return None


def resolve_vanity(username: str, api_key: str) -> str | None:
    if not api_key:
        return None
    data = _api_get(
        "https://api.steampowered.com/ISteamUser/ResolveVanityURL/v1/",
        {"key": api_key, "vanityurl": username},
    )
    if data.get("response", {}).get("success") == 1:
        return str(data["response"]["steamid"])
    return None


def _api_get(url: str, params: dict[str, Any]) -> dict:
    last_error = "Steam API не вернул ответ"
    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        try:
            r = _session.get(url, params=params, timeout=20)
            if r.status_code in RETRYABLE_HTTP_CODES:
                last_error = f"Steam API временно недоступен (HTTP {r.status_code})"
                if attempt >= MAX_REQUEST_ATTEMPTS:
                    break
                retry_after = r.headers.get("Retry-After", "")
                try:
                    delay = float(retry_after)
                except (TypeError, ValueError):
                    delay = float(attempt)
                delay = max(0.25, min(10.0, delay))
                logger.warning(
                    "[HTTP:Steam] HTTP %d; повтор %d/%d через %.1f с",
                    r.status_code, attempt + 1, MAX_REQUEST_ATTEMPTS, delay,
                )
                time.sleep(delay)
                continue
            if r.status_code != 200:
                logger.warning("[HTTP:Steam] Запрос отклонён: HTTP %d", r.status_code)
                raise RuntimeError(f"Steam API отклонил запрос (HTTP {r.status_code})")
            data = r.json()
            if not isinstance(data, dict):
                raise ValueError("ответ не является объектом")
            if attempt > 1:
                logger.info("[HTTP:Steam] Ответ получен с попытки %d/%d", attempt, MAX_REQUEST_ATTEMPTS)
            return data
        except requests.RequestException as exc:
            last_error = f"Нет соединения со Steam API: {exc}"
            if attempt >= MAX_REQUEST_ATTEMPTS:
                break
            logger.warning(
                "[HTTP:Steam] %s; повтор %d/%d",
                type(exc).__name__, attempt + 1, MAX_REQUEST_ATTEMPTS,
            )
            time.sleep(float(attempt))
        except ValueError as exc:
            last_error = f"Steam API вернул некорректный JSON: {exc}"
            if attempt >= MAX_REQUEST_ATTEMPTS:
                break
            time.sleep(float(attempt))
    logger.error("[HTTP:Steam] %s после %d попыток", last_error, MAX_REQUEST_ATTEMPTS)
    raise requests.ConnectionError(f"{last_error} после {MAX_REQUEST_ATTEMPTS} попыток")


def get_player_summaries(steamids: list[str], api_key: str) -> dict[str, dict]:
    if not steamids or not api_key:
        return {}
    data = _api_get(
        "https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/",
        {"key": api_key, "steamids": ",".join(steamids)},
    )
    return {p["steamid"]: p for p in data.get("response", {}).get("players", [])}


def get_player_bans(steamids: list[str], api_key: str) -> dict[str, dict]:
    if not steamids or not api_key:
        return {}
    data = _api_get(
        "https://api.steampowered.com/ISteamUser/GetPlayerBans/v1/",
        {"key": api_key, "steamids": ",".join(steamids)},
    )
    return {str(p["SteamId"]): p for p in data.get("players", [])}


def is_kt_flag(summary: dict | None, bans: dict | None) -> tuple[bool, str]:
    """True = есть признаки КТ / проблемы (как в Main3)."""
    if summary is None:
        return True, "не найден в Steam API"
    if bans:
        if bans.get("CommunityBanned"):
            return True, "CommunityBanned"
        eco = bans.get("EconomyBan", "none")
        if eco and str(eco).lower() != "none":
            return True, f"EconomyBan={eco}"
    vis = summary.get("communityvisibilitystate", 1)
    if vis < VALID_VISIBILITY:
        labels = {1: "закрыт", 2: "только друзья"}
        return True, f"профиль {labels.get(vis, f'visibility={vis}')}"
    if summary.get("profilestate", 0) != 1:
        return True, "профиль не настроен"
    return False, ""


def steam_kt_passes(api_key: str, steam_profile_url: str) -> tuple[bool, str]:
    """
    Возвращает (успех_без_КТ, причина_при_провале).
    Успех: аккаунт не попадает под критерии КТ из Main3.
    """
    if not api_key.strip():
        return False, "нет steam_web_api_key в конфиге"
    sid = extract_steamid64(steam_profile_url.strip(), api_key)
    if not sid:
        return False, "не удалось извлечь SteamID из ссылки"
    summaries = get_player_summaries([sid], api_key)
    bans_map = get_player_bans([sid], api_key)
    summary = summaries.get(sid)
    bans = bans_map.get(sid)
    bad, reason = is_kt_flag(summary, bans)
    if bad:
        return False, reason or "КТ"
    return True, ""
