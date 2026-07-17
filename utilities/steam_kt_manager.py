from __future__ import annotations

import re
import threading
import time
from collections import Counter, deque
from datetime import datetime
from typing import Any, Callable

import requests

from autoarb.core.lzt_common import api_base
from autoarb.core.throttled_client import ThrottledClient, use_lzt_proxy_from_config

from .resale_finder import extract_bulk_item_map


BATCH_SIZE = 100
LZT_BULK_SIZE = 250
LZT_TAGS_LIMIT = 5_000
MAX_INPUT_CHARS = 2_000_000
MAX_ROWS = 50_000
MAX_ATTEMPTS = 3
RETRYABLE_HTTP = {408, 425, 429, 500, 502, 503, 504}

STEAM_PROFILE_RE = re.compile(
    r"https?://(?:www\.)?steamcommunity\.com/profiles/(\d{15,20})(?:[/?#][^\s]*)?",
    re.IGNORECASE,
)
LZT_ITEM_RE = re.compile(
    r"https?://(?:www\.)?(?:lzt\.market|zelenka\.guru)/(\d+)(?:[/?#][^\s]*)?",
    re.IGNORECASE,
)


class SteamAPIKeyError(ValueError):
    pass


def parse_accounts(text: str) -> list[dict[str, Any]]:
    """Extract Steam profile links from arbitrary lines without persisting secrets."""
    if len(text) > MAX_INPUT_CHARS:
        raise ValueError("Текст слишком большой: максимум 2 МБ за один запуск")
    rows: list[dict[str, Any]] = []
    for line_number, raw in enumerate(text.splitlines(), start=1):
        source_line = raw.strip()
        if not source_line:
            continue
        steam_matches = list(STEAM_PROFILE_RE.finditer(source_line))
        lzt_match = LZT_ITEM_RE.search(source_line)
        lzt_url = f"https://lzt.market/{lzt_match.group(1)}/" if lzt_match else ""
        lzt_item_id = int(lzt_match.group(1)) if lzt_match else None
        if not steam_matches:
            rows.append({
                "line": line_number,
                "source_line": source_line,
                "steam_id": "",
                "steam_url": "",
                "lzt_url": lzt_url,
                "lzt_item_id": lzt_item_id,
                "status": "invalid",
                "reason": "Steam-ссылка не найдена",
            })
            if len(rows) > MAX_ROWS:
                raise ValueError(f"За один запуск можно проверить не больше {MAX_ROWS} строк")
            continue
        for steam_match in steam_matches:
            steam_id = steam_match.group(1)
            rows.append({
                "line": line_number,
                "source_line": source_line,
                "steam_id": steam_id,
                "steam_url": f"https://steamcommunity.com/profiles/{steam_id}",
                "lzt_url": lzt_url,
                "lzt_item_id": lzt_item_id,
                "status": "pending",
                "reason": "Ожидает проверки",
            })
        if len(rows) > MAX_ROWS:
            raise ValueError(f"За один запуск можно проверить не больше {MAX_ROWS} строк")
    return rows


def classify_kt(summary: dict[str, Any] | None, bans: dict[str, Any] | None) -> tuple[str, str, str]:
    """Return (status, reason, kind) using the supplied KT-checker rules."""
    if summary is None:
        return "kt", "Профиль не найден в Steam API", "missing"
    if bans:
        if bool(bans.get("CommunityBanned")):
            return "kt", "CommunityBanned", "community_ban"
        economy_ban = str(bans.get("EconomyBan") or "none")
        if economy_ban.lower() != "none":
            return "kt", f"EconomyBan={economy_ban}", "economy_ban"
    try:
        visibility = int(summary.get("communityvisibilitystate", 1))
    except (TypeError, ValueError):
        visibility = 1
    if visibility < 3:
        labels = {1: "профиль закрыт", 2: "профиль только для друзей"}
        kinds = {1: "private", 2: "friends_only"}
        return "kt", labels.get(visibility, f"visibility={visibility}"), kinds.get(visibility, "visibility")
    try:
        profile_state = int(summary.get("profilestate", 0))
    except (TypeError, ValueError):
        profile_state = 0
    if profile_state != 1:
        return "kt", "Профиль не настроен", "unconfigured"
    return "safe", "Признаки КТ не найдены", "safe"


class SteamKTManagerService:
    def __init__(self, config_loader: Callable[[], dict[str, Any]]) -> None:
        self._config_loader = config_loader
        self._lock = threading.RLock()
        self._cancel = threading.Event()
        self._thread: threading.Thread | None = None
        self._seller_cancel = threading.Event()
        self._seller_thread: threading.Thread | None = None
        self._logs: deque[dict[str, str]] = deque(maxlen=300)
        self._results: list[dict[str, Any]] = []
        self._revision = 0
        self._state = self._empty_state()
        self._seller_state = self._empty_seller_state()
        self._tag_state = self._empty_tag_state()

    @staticmethod
    def _empty_state() -> dict[str, Any]:
        return {
            "running": False,
            "status": "idle",
            "error": None,
            "started_at": None,
            "finished_at": None,
            "total": 0,
            "processed": 0,
            "total_batches": 0,
            "batches_done": 0,
            "api_requests": 0,
        }

    @staticmethod
    def _empty_seller_state() -> dict[str, Any]:
        return {
            "running": False,
            "status": "idle",
            "error": None,
            "phase": None,
            "started_at": None,
            "finished_at": None,
            "total_items": 0,
            "processed_items": 0,
            "total_batches": 0,
            "batches_done": 0,
            "phase_total_batches": 0,
            "phase_batches_done": 0,
            "api_requests": 0,
        }

    @staticmethod
    def _empty_tag_state() -> dict[str, Any]:
        return {
            "running": False,
            "status": "idle",
            "error": None,
            "started_at": None,
            "finished_at": None,
            "total_items": 0,
            "processed_items": 0,
            "tag_id": None,
        }

    def _any_running_locked(self) -> bool:
        return bool(
            self._state.get("running")
            or self._seller_state.get("running")
            or self._tag_state.get("running")
        )

    def _log(self, message: str, level: str = "info") -> None:
        with self._lock:
            self._logs.append({
                "at": datetime.now().strftime("%H:%M:%S"),
                "level": level,
                "message": message,
            })

    def _summary_locked(self) -> dict[str, int]:
        counts = Counter(str(row.get("status") or "pending") for row in self._results)
        return {
            "total": len(self._results),
            "safe": counts["safe"],
            "kt": counts["kt"],
            "errors": counts["error"] + counts["invalid"],
            "invalid": counts["invalid"],
            "pending": counts["pending"],
        }

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "ok": True,
                **dict(self._state),
                "revision": self._revision,
                "summary": self._summary_locked(),
                "logs": list(self._logs),
                "batch_size": BATCH_SIZE,
                "operations": {
                    "sellers": dict(self._seller_state),
                    "tags": dict(self._tag_state),
                },
            }

    def results(self) -> dict[str, Any]:
        with self._lock:
            return {"ok": True, "revision": self._revision, "results": [dict(row) for row in self._results]}

    def start(self, text: str) -> dict[str, Any]:
        config = self._config_loader() or {}
        api_key = str(config.get("steam_web_api_key") or "").strip()
        if not api_key:
            raise ValueError("Сначала добавь Steam Web API Key в настройках проверки → Steam")
        rows = parse_accounts(str(text or ""))
        valid_rows = [row for row in rows if row["steam_id"]]
        if not valid_rows:
            raise ValueError("Не найдено ни одной ссылки steamcommunity.com/profiles/SteamID64")
        unique_ids = list(dict.fromkeys(row["steam_id"] for row in valid_rows))
        total_batches = (len(unique_ids) + BATCH_SIZE - 1) // BATCH_SIZE
        with self._lock:
            if self._any_running_locked():
                raise RuntimeError("Проверка KT уже выполняется")
            self._cancel.clear()
            self._logs.clear()
            self._results = rows
            self._tag_state = self._empty_tag_state()
            invalid_count = sum(row["status"] == "invalid" for row in rows)
            self._revision += 1
            self._state = {
                **self._empty_state(),
                "running": True,
                "status": "running",
                "started_at": datetime.now().isoformat(timespec="seconds"),
                "total": len(rows),
                "processed": invalid_count,
                "total_batches": total_batches,
            }
            self._thread = threading.Thread(
                target=self._run,
                args=(api_key, unique_ids),
                name="utility-steam-kt-manager",
                daemon=True,
            )
            self._thread.start()
        return self.status()

    def _request_json(self, session: requests.Session, url: str, api_key: str, steam_ids: list[str], label: str) -> dict[str, Any]:
        last_error = "Steam API не вернул ответ"
        for attempt in range(1, MAX_ATTEMPTS + 1):
            if self._cancel.is_set():
                raise InterruptedError("Проверка остановлена")
            try:
                with self._lock:
                    self._state["api_requests"] += 1
                response = session.get(
                    url,
                    params={"key": api_key, "steamids": ",".join(steam_ids)},
                    timeout=25,
                )
                if response.status_code in {401, 403}:
                    raise SteamAPIKeyError(f"Steam API отклонил ключ (HTTP {response.status_code})")
                if response.status_code in RETRYABLE_HTTP:
                    last_error = f"{label}: временная ошибка HTTP {response.status_code}"
                    if attempt < MAX_ATTEMPTS:
                        retry_after = response.headers.get("Retry-After")
                        try:
                            delay = float(retry_after)
                        except (TypeError, ValueError):
                            delay = float(attempt)
                        delay = max(0.5, min(60.0, delay))
                        self._log(f"{last_error}. Повтор {attempt + 1}/{MAX_ATTEMPTS} через {delay:g} сек.", "warning")
                        if self._cancel.wait(delay):
                            raise InterruptedError("Проверка остановлена")
                        continue
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError(f"{label}: Steam API вернул некорректный JSON")
                return payload
            except SteamAPIKeyError:
                raise
            except (requests.RequestException, TypeError, ValueError) as exc:
                last_error = f"{label}: {type(exc).__name__}: {exc}"
                if attempt < MAX_ATTEMPTS:
                    self._log(f"{last_error}. Повтор {attempt + 1}/{MAX_ATTEMPTS}.", "warning")
                    if self._cancel.wait(float(attempt)):
                        raise InterruptedError("Проверка остановлена")
                    continue
        raise ConnectionError(f"{last_error} после {MAX_ATTEMPTS} попыток")

    def _apply_batch_error(self, steam_ids: list[str], message: str) -> int:
        id_set = set(steam_ids)
        affected = 0
        with self._lock:
            for row in self._results:
                if row.get("steam_id") in id_set and row.get("status") == "pending":
                    row["status"] = "error"
                    row["reason"] = message
                    affected += 1
            self._state["processed"] += affected
            self._state["batches_done"] += 1
            self._revision += 1
        return affected

    def _run(self, api_key: str, unique_ids: list[str]) -> None:
        session = requests.Session()
        session.headers.update({"Accept": "application/json", "User-Agent": "LZT-Control-Steam-KT/1.0"})
        try:
            total_batches = (len(unique_ids) + BATCH_SIZE - 1) // BATCH_SIZE
            duplicate_count = sum(1 for row in self._results if row.get("steam_id")) - len(unique_ids)
            self._log(
                f"Проверка начата · {len(unique_ids)} уникальных SteamID · {total_batches} пачек по {BATCH_SIZE}"
            )
            invalid_count = sum(1 for row in self._results if row.get("status") == "invalid")
            if invalid_count:
                self._log(f"Строк без Steam-ссылки: {invalid_count}. Они пропущены.", "warning")
            if duplicate_count:
                self._log(f"Повторяющихся SteamID: {duplicate_count}. API проверит каждый ID один раз.")
            for batch_index in range(total_batches):
                if self._cancel.is_set():
                    break
                batch = unique_ids[batch_index * BATCH_SIZE:(batch_index + 1) * BATCH_SIZE]
                self._log(f"Пачка {batch_index + 1}/{total_batches} · проверяем {len(batch)} SteamID")
                try:
                    summaries_payload = self._request_json(
                        session,
                        "https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/",
                        api_key,
                        batch,
                        "Профили Steam",
                    )
                    bans_payload = self._request_json(
                        session,
                        "https://api.steampowered.com/ISteamUser/GetPlayerBans/v1/",
                        api_key,
                        batch,
                        "Блокировки Steam",
                    )
                    summaries = {
                        str(player.get("steamid")): player
                        for player in summaries_payload.get("response", {}).get("players", [])
                        if isinstance(player, dict) and player.get("steamid")
                    }
                    bans = {
                        str(player.get("SteamId")): player
                        for player in bans_payload.get("players", [])
                        if isinstance(player, dict) and player.get("SteamId")
                    }
                    id_results = {steam_id: classify_kt(summaries.get(steam_id), bans.get(steam_id)) for steam_id in batch}
                    processed = 0
                    with self._lock:
                        for row in self._results:
                            result = id_results.get(str(row.get("steam_id") or ""))
                            if result and row.get("status") == "pending":
                                row["status"], row["reason"], row["kt_type"] = result
                                processed += 1
                        self._state["processed"] += processed
                        self._state["batches_done"] += 1
                        self._revision += 1
                    kt_count = sum(status == "kt" for status, _, _ in id_results.values())
                    self._log(
                        f"Пачка {batch_index + 1}/{total_batches} готова · KT: {kt_count} · без KT: {len(batch) - kt_count}"
                    )
                except InterruptedError:
                    break
                except SteamAPIKeyError:
                    raise
                except Exception as exc:
                    message = f"Не удалось проверить пачку: {exc}"
                    affected = self._apply_batch_error(batch, message)
                    self._log(f"Пачка {batch_index + 1}/{total_batches}: {message} · строк: {affected}", "error")
                if batch_index + 1 < total_batches and not self._cancel.wait(0.5):
                    continue
            with self._lock:
                cancelled = self._cancel.is_set()
                self._state.update({
                    "running": False,
                    "status": "cancelled" if cancelled else "completed",
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                })
            summary = self.status()["summary"]
            if cancelled:
                self._log(f"Проверка остановлена · обработано {self._state['processed']} из {self._state['total']}", "warning")
            else:
                self._log(
                    f"Проверка завершена · KT: {summary['kt']} · без KT: {summary['safe']} · ошибок: {summary['errors']}"
                )
        except Exception as exc:
            message = str(exc) or type(exc).__name__
            with self._lock:
                for row in self._results:
                    if row.get("status") == "pending":
                        row["status"] = "error"
                        row["reason"] = message
                self._state.update({
                    "running": False,
                    "status": "error",
                    "error": message,
                    "processed": len(self._results),
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                })
                self._revision += 1
            self._log(message, "error")
        finally:
            session.close()

    def cancel(self) -> dict[str, Any]:
        self._cancel.set()
        self._log("Получена команда остановки; завершаем текущий запрос…", "warning")
        return self.status()

    @staticmethod
    def _same_item_ids(item: dict[str, Any]) -> list[int]:
        raw: Any = None
        for key in ("sameItemsIds", "sameItemIds", "same_items_ids"):
            if key in item:
                raw = item.get(key)
                break
        values: set[int] = set()
        if isinstance(raw, list):
            for value in raw:
                try:
                    item_id = int(value)
                except (TypeError, ValueError):
                    continue
                if item_id > 0:
                    values.add(item_id)
        return sorted(values)

    @classmethod
    def _previous_item_id(cls, current_id: int, item: dict[str, Any]) -> int | None:
        """Return the nearest resale-chain ID that existed before current_id."""
        previous_ids = [item_id for item_id in cls._same_item_ids(item) if item_id < current_id]
        return max(previous_ids, default=None)

    def _seller_bulk_request(
        self,
        client: ThrottledClient,
        endpoint: str,
        item_ids: list[int],
        use_proxy: bool,
    ) -> dict[str, dict[str, Any]]:
        if self._seller_cancel.is_set():
            raise InterruptedError("Получение продавцов остановлено")
        response = client.post(
            endpoint,
            json_body={"item_id": item_ids, "parse_same_item_ids": True},
            use_proxy=use_proxy,
            retry_safe=True,
        )
        with self._lock:
            self._seller_state["api_requests"] += 1
        if not 200 <= response.status_code < 300:
            try:
                payload = response.json()
                detail = (
                    payload.get("errors") or payload.get("error") or payload.get("message") or payload
                    if isinstance(payload, dict)
                    else payload
                )
            except (TypeError, ValueError):
                detail = response.text[:300]
            raise ValueError(f"LZT API вернул HTTP {response.status_code}: {detail}")
        try:
            payload = response.json()
        except ValueError as exc:
            raise ValueError("LZT API вернул повреждённый JSON") from exc
        return extract_bulk_item_map(payload)

    def start_sellers(self, token: str) -> dict[str, Any]:
        token = str(token or "").strip()
        if not token:
            raise ValueError("Выбери LZT-аккаунт с API-токеном")
        with self._lock:
            if self._any_running_locked():
                raise RuntimeError("Дождись завершения текущей операции утилиты")
            current_ids = list(dict.fromkeys(
                int(row["lzt_item_id"])
                for row in self._results
                if row.get("lzt_item_id") and row.get("status") == "kt"
            ))
            if not current_ids:
                raise ValueError("Среди KT-аккаунтов нет LZT-ссылок для получения продавцов")
            for row in self._results:
                row.pop("previous_item_id", None)
                row.pop("seller_user_id", None)
                row.pop("seller_username", None)
                row.pop("seller_error", None)
            self._tag_state = self._empty_tag_state()
            self._seller_cancel.clear()
            self._seller_state = {
                **self._empty_seller_state(),
                "running": True,
                "status": "running",
                "phase": "chains",
                "started_at": datetime.now().isoformat(timespec="seconds"),
                "total_items": len(current_ids),
                # Two read-only bulk stages: relations, then seller profiles.
                # The exact second-stage count is corrected after stage one.
                "total_batches": 2 * ((len(current_ids) + LZT_BULK_SIZE - 1) // LZT_BULK_SIZE),
                "phase_total_batches": (len(current_ids) + LZT_BULK_SIZE - 1) // LZT_BULK_SIZE,
            }
            self._revision += 1
            self._seller_thread = threading.Thread(
                target=self._run_sellers,
                args=(token, current_ids),
                name="utility-steam-kt-sellers",
                daemon=True,
            )
            self._seller_thread.start()
        return self.status()

    def _run_sellers(self, token: str, current_ids: list[int]) -> None:
        try:
            cfg = dict(self._config_loader() or {})
            cfg["token"] = token
            enabled_proxy, proxy = use_lzt_proxy_from_config(cfg)
            client = ThrottledClient(
                token,
                proxy,
                delay_seconds=max(0.0, float(cfg.get("request_delay_seconds", 3) or 3)),
                use_lzt_proxy=enabled_proxy,
                source="Утилита: Steam КТ",
            )
            endpoint = f"{api_base(cfg)}/bulk/items"
            current_to_previous: dict[int, int | None] = {}
            chain_batches = [current_ids[index:index + LZT_BULK_SIZE] for index in range(0, len(current_ids), LZT_BULK_SIZE)]
            self._log(f"Продавцы · этап 1/2: ищем предыдущие ID для {len(current_ids)} аккаунтов · пачек {len(chain_batches)}")
            for batch_index, batch in enumerate(chain_batches, start=1):
                item_map = self._seller_bulk_request(client, endpoint, batch, enabled_proxy)
                for current_id in batch:
                    item = item_map.get(str(current_id), {})
                    current_to_previous[current_id] = self._previous_item_id(current_id, item)
                with self._lock:
                    self._seller_state["batches_done"] = int(self._seller_state.get("batches_done") or 0) + 1
                    self._seller_state["phase_batches_done"] = int(self._seller_state.get("phase_batches_done") or 0) + 1
                self._log(f"Продавцы · этап 1/2: пачка {batch_index}/{len(chain_batches)} готова")

            previous_ids = list(dict.fromkeys(value for value in current_to_previous.values() if value is not None))
            seller_batches = [previous_ids[index:index + LZT_BULK_SIZE] for index in range(0, len(previous_ids), LZT_BULK_SIZE)]
            with self._lock:
                self._seller_state["phase"] = "sellers"
                self._seller_state["total_batches"] = len(chain_batches) + len(seller_batches)
                self._seller_state["phase_total_batches"] = len(seller_batches)
                self._seller_state["phase_batches_done"] = 0
            self._log(f"Продавцы · этап 2/2: получаем данные {len(previous_ids)} исходных покупок · пачек {len(seller_batches)}")
            sellers_by_previous: dict[int, dict[str, Any]] = {}
            for batch_index, batch in enumerate(seller_batches, start=1):
                item_map = self._seller_bulk_request(client, endpoint, batch, enabled_proxy)
                for previous_id in batch:
                    item = item_map.get(str(previous_id), {})
                    seller = item.get("seller") if isinstance(item.get("seller"), dict) else {}
                    try:
                        user_id = int(seller.get("user_id") or 0)
                    except (TypeError, ValueError):
                        user_id = 0
                    if user_id:
                        sellers_by_previous[previous_id] = {
                            "user_id": user_id,
                            "username": str(seller.get("username") or f"ID {user_id}"),
                        }
                with self._lock:
                    self._seller_state["batches_done"] = int(self._seller_state.get("batches_done") or 0) + 1
                    self._seller_state["phase_batches_done"] = int(self._seller_state.get("phase_batches_done") or 0) + 1
                self._log(f"Продавцы · этап 2/2: пачка {batch_index}/{len(seller_batches)} готова")

            found_ids: set[int] = set()
            with self._lock:
                for row in self._results:
                    try:
                        current_id = int(row.get("lzt_item_id") or 0)
                    except (TypeError, ValueError):
                        current_id = 0
                    if not current_id:
                        continue
                    previous_id = current_to_previous.get(current_id)
                    row["previous_item_id"] = previous_id
                    seller = sellers_by_previous.get(previous_id or 0)
                    if seller:
                        row["seller_user_id"] = seller["user_id"]
                        row["seller_username"] = seller["username"]
                        found_ids.add(current_id)
                    elif previous_id is None:
                        row["seller_error"] = "Предыдущий ID покупки не найден"
                    else:
                        row["seller_error"] = "Продавец не указан в ответе API"
                cancelled = self._seller_cancel.is_set()
                self._seller_state.update({
                    "running": False,
                    "status": "cancelled" if cancelled else "completed",
                    "phase": None,
                    "processed_items": len(current_ids),
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                })
                self._revision += 1
            if cancelled:
                self._log("Получение продавцов остановлено", "warning")
            else:
                self._log(f"Данные продавцов получены · найдено {len(found_ids)} из {len(current_ids)}")
        except InterruptedError:
            with self._lock:
                self._seller_state.update({
                    "running": False,
                    "status": "cancelled",
                    "phase": None,
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                })
            self._log("Получение продавцов остановлено", "warning")
        except Exception as exc:
            message = str(exc) or type(exc).__name__
            with self._lock:
                self._seller_state.update({
                    "running": False,
                    "status": "error",
                    "error": message,
                    "phase": None,
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                })
                self._revision += 1
            self._log(f"Не удалось получить продавцов: {message}", "error")

    def cancel_sellers(self) -> dict[str, Any]:
        self._seller_cancel.set()
        self._log("Останавливаем получение данных продавцов…", "warning")
        return self.status()

    def add_tag(self, token: str, item_ids: list[int], tag_id: int) -> dict[str, Any]:
        token = str(token or "").strip()
        if not token:
            raise ValueError("Выбери LZT-аккаунт с API-токеном")
        try:
            normalized_tag = int(tag_id)
        except (TypeError, ValueError) as exc:
            raise ValueError("Укажи корректный ID метки") from exc
        if normalized_tag < 1:
            raise ValueError("ID метки должен быть больше нуля")

        normalized_ids: list[int] = []
        seen_ids: set[int] = set()
        for value in item_ids or []:
            try:
                item_id = int(value)
            except (TypeError, ValueError):
                continue
            if item_id > 0 and item_id not in seen_ids:
                seen_ids.add(item_id)
                normalized_ids.append(item_id)
        if not normalized_ids:
            raise ValueError("В текущей выборке нет KT-аккаунтов с LZT-ссылками")
        if len(normalized_ids) > LZT_TAGS_LIMIT:
            raise ValueError(f"За один запрос можно обработать не больше {LZT_TAGS_LIMIT} аккаунтов")

        with self._lock:
            if self._any_running_locked():
                raise RuntimeError("Дождись завершения текущей операции утилиты")
            allowed_ids = {
                int(row["lzt_item_id"])
                for row in self._results
                if row.get("status") == "kt" and row.get("lzt_item_id")
            }
            rejected = [item_id for item_id in normalized_ids if item_id not in allowed_ids]
            if rejected:
                raise ValueError("Выборка устарела: обнови фильтры и повтори действие")
            self._tag_state = {
                **self._empty_tag_state(),
                "running": True,
                "status": "running",
                "started_at": datetime.now().isoformat(timespec="seconds"),
                "total_items": len(normalized_ids),
                "tag_id": normalized_tag,
            }

        try:
            cfg = dict(self._config_loader() or {})
            cfg["token"] = token
            enabled_proxy, proxy = use_lzt_proxy_from_config(cfg)
            client = ThrottledClient(
                token,
                proxy,
                delay_seconds=max(0.0, float(cfg.get("request_delay_seconds", 3) or 3)),
                use_lzt_proxy=enabled_proxy,
                source="Утилита: Steam КТ · метки",
            )
            response = client.post(
                f"{api_base(cfg)}/items/bulk-action",
                json_body={
                    "item_ids": normalized_ids,
                    "action": "edit-tags",
                    "add_tags": [normalized_tag],
                    "remove_tags": [],
                },
                use_proxy=enabled_proxy,
                # Повтор безопасен: повторное добавление той же метки идемпотентно.
                retry_safe=True,
            )
            try:
                payload = response.json()
            except ValueError:
                payload = None
            errors = payload.get("errors") if isinstance(payload, dict) else None
            failed_status = str(payload.get("status") or "").casefold() if isinstance(payload, dict) else ""
            if not 200 <= response.status_code < 300 or errors or failed_status in {"error", "failed", "fail"}:
                detail = errors or (payload.get("message") if isinstance(payload, dict) else None) or response.text[:300]
                raise ValueError(f"LZT API не добавил метку: {detail}")

            with self._lock:
                self._tag_state.update({
                    "running": False,
                    "status": "completed",
                    "processed_items": len(normalized_ids),
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                })
            self._log(
                f"Метка {normalized_tag} добавлена · KT-аккаунтов: {len(normalized_ids)}"
            )
            return {
                "ok": True,
                "tag_id": normalized_tag,
                "processed_items": len(normalized_ids),
            }
        except Exception as exc:
            message = str(exc) or type(exc).__name__
            with self._lock:
                self._tag_state.update({
                    "running": False,
                    "status": "error",
                    "error": message,
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                })
            self._log(f"Не удалось добавить метку: {message}", "error")
            if isinstance(exc, (TypeError, ValueError)):
                raise
            raise ValueError(f"Не удалось добавить метку: {message}") from exc

    def clear(self) -> dict[str, Any]:
        with self._lock:
            if self._any_running_locked():
                raise RuntimeError("Сначала останови проверку KT")
            self._results = []
            self._logs.clear()
            self._revision += 1
            self._state = self._empty_state()
            self._seller_state = self._empty_seller_state()
            self._tag_state = self._empty_tag_state()
        return self.status()

    def stop(self) -> None:
        self._cancel.set()
        self._seller_cancel.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=5)
        seller_thread = self._seller_thread
        if seller_thread and seller_thread.is_alive():
            seller_thread.join(timeout=5)
