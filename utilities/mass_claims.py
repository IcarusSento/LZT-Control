from __future__ import annotations

import json
import re
import threading
from collections import Counter, deque
from datetime import datetime
from typing import Any, Callable

import requests

from autoarb.core.lzt_claims import mark_claim_sent, post_claim, wait_claim_rate_limit
from autoarb.core.lzt_common import api_base
from autoarb.core.throttled_client import ThrottledClient, use_lzt_proxy_from_config

from .resale_finder import extract_bulk_item_map


BULK_ITEMS_LIMIT = 250
MAX_INPUT_CHARS = 2_000_000
MAX_ITEMS = 10_000
MIN_CLAIM_INTERVAL_SECONDS = 60
MAX_CLAIM_INTERVAL_SECONDS = 3_600
ITEM_URL_RE = re.compile(
    r"(?:https?://)?(?:www\.)?(?:lzt\.market|zelenka\.guru|lolz\.guru)/"
    r"(?!(?:user|orders|market|threads|claims?)(?:/|$))"
    r"(?:[a-z][a-z0-9_-]+/)?(\d+)(?:[/?#]|$)",
    re.IGNORECASE,
)


def parse_item_ids(text: str) -> list[int]:
    value = str(text or "")
    if len(value) > MAX_INPUT_CHARS:
        raise ValueError("Текст слишком большой: максимум 2 МБ")
    result: list[int] = []
    seen: set[int] = set()
    for raw_line in value.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        matches = [int(match.group(1)) for match in ITEM_URL_RE.finditer(line)]
        if not matches and line.isdigit():
            matches = [int(line)]
        for item_id in matches:
            if item_id <= 0 or item_id in seen:
                continue
            seen.add(item_id)
            result.append(item_id)
            if len(result) > MAX_ITEMS:
                raise ValueError(f"За один запуск можно обработать не больше {MAX_ITEMS} аккаунтов")
    return result


def previous_item_id(current_id: int, item: dict[str, Any]) -> int | None:
    raw: Any = None
    for key in ("sameItemsIds", "sameItemIds", "same_items_ids"):
        if key in item:
            raw = item.get(key)
            break
    values: set[int] = set()
    if isinstance(raw, list):
        for value in raw:
            try:
                candidate = int(value)
            except (TypeError, ValueError):
                continue
            if 0 < candidate < current_id:
                values.add(candidate)
    return max(values, default=None)


def short_response(response: requests.Response) -> str:
    try:
        payload = response.json()
        if isinstance(payload, dict):
            detail = payload.get("errors") or payload.get("error") or payload.get("message")
            if detail:
                return json.dumps(detail, ensure_ascii=False)[:800]
        return f"HTTP {response.status_code} без описания ошибки"
    except (TypeError, ValueError):
        return str(response.text or "")[:800]


class MassClaimsService:
    """Prepare and execute a local, explicitly confirmed queue of LZT claims."""

    def __init__(self, config_loader: Callable[[], dict[str, Any]]) -> None:
        self._config_loader = config_loader
        self._lock = threading.RLock()
        self._cancel = threading.Event()
        self._thread: threading.Thread | None = None
        self._logs: deque[dict[str, str]] = deque(maxlen=500)
        self._results: list[dict[str, Any]] = []
        self._description = ""
        self._revision = 0
        self._state = self._empty_state()

    @staticmethod
    def _empty_state() -> dict[str, Any]:
        return {
            "running": False,
            "status": "idle",
            "phase": None,
            "error": None,
            "started_at": None,
            "finished_at": None,
            "use_previous_id": True,
            "interval_seconds": 63,
            "total": 0,
            "processed": 0,
            "total_batches": 0,
            "batches_done": 0,
            "api_requests": 0,
        }

    def _log(self, message: str, level: str = "info") -> None:
        with self._lock:
            self._logs.append({
                "at": datetime.now().strftime("%H:%M:%S"),
                "level": level,
                "message": str(message),
            })

    def _summary_locked(self) -> dict[str, int]:
        counts = Counter(str(row.get("status") or "pending") for row in self._results)
        return {
            "total": len(self._results),
            "ready": counts["ready"],
            "created": counts["created"],
            "errors": counts["error"],
            "unresolved": counts["unresolved"] + counts["duplicate"],
            "pending": counts["pending"] + counts["resolving"] + counts["creating"],
        }

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "ok": True,
                **dict(self._state),
                "revision": self._revision,
                "summary": self._summary_locked(),
                "logs": list(self._logs),
                "bulk_limit": BULK_ITEMS_LIMIT,
            }

    def results(self) -> dict[str, Any]:
        with self._lock:
            return {
                "ok": True,
                "revision": self._revision,
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
            source="Утилита: массовые арбитражи",
        )
        return client, enabled_proxy, cfg

    def prepare(
        self,
        token: str,
        text: str,
        description: str,
        use_previous_id: bool,
        interval_seconds: int,
    ) -> dict[str, Any]:
        token = str(token or "").strip()
        description = str(description or "").strip()
        if not token:
            raise ValueError("Выбери LZT-аккаунт с API-токеном")
        if not description:
            raise ValueError("Добавь описание арбитража")
        if len(description) > 4_000:
            raise ValueError("Описание арбитража не должно превышать 4000 символов")
        ids = parse_item_ids(text)
        if not ids:
            raise ValueError("Не найдено ни одной ссылки LZT или item_id")
        interval = max(MIN_CLAIM_INTERVAL_SECONDS, min(MAX_CLAIM_INTERVAL_SECONDS, int(interval_seconds)))
        with self._lock:
            if self._state.get("running"):
                raise RuntimeError("Дождись завершения текущей операции")
            self._cancel.clear()
            self._logs.clear()
            self._description = description
            self._results = [
                {
                    "source_id": str(item_id),
                    "target_id": None if use_previous_id else str(item_id),
                    "status": "resolving" if use_previous_id else "ready",
                    "message": "Ищем предыдущий ID" if use_previous_id else "Готов к созданию",
                    "http_status": None,
                    "api_response": "",
                }
                for item_id in ids
            ]
            self._revision += 1
            batches = (len(ids) + BULK_ITEMS_LIMIT - 1) // BULK_ITEMS_LIMIT if use_previous_id else 0
            self._state = {
                **self._empty_state(),
                "running": bool(use_previous_id),
                "status": "running" if use_previous_id else "ready",
                "phase": "resolve" if use_previous_id else None,
                "started_at": datetime.now().isoformat(timespec="seconds"),
                "finished_at": None if use_previous_id else datetime.now().isoformat(timespec="seconds"),
                "use_previous_id": bool(use_previous_id),
                "interval_seconds": interval,
                "total": len(ids),
                "processed": 0 if use_previous_id else len(ids),
                "total_batches": batches,
            }
            if use_previous_id:
                self._thread = threading.Thread(
                    target=self._resolve_previous_ids,
                    args=(token, ids),
                    name="utility-mass-claims-resolve",
                    daemon=True,
                )
                self._thread.start()
            else:
                self._log(f"Очередь подготовлена · {len(ids)} аккаунтов · используются исходные ID")
        return self.status()

    def _resolve_previous_ids(self, token: str, ids: list[int]) -> None:
        try:
            client, enabled_proxy, cfg = self._client(token)
            endpoint = f"{api_base(cfg)}/bulk/items"
            batches = [ids[index:index + BULK_ITEMS_LIMIT] for index in range(0, len(ids), BULK_ITEMS_LIMIT)]
            self._log(f"Same ID · {len(ids)} аккаунтов · {len(batches)} пачек по {BULK_ITEMS_LIMIT}")
            for batch_index, batch in enumerate(batches, start=1):
                if self._cancel.is_set():
                    raise InterruptedError
                self._log(f"Same ID · пачка {batch_index}/{len(batches)} · {len(batch)} аккаунтов")
                response = client.post(
                    endpoint,
                    json_body={"item_id": batch, "parse_same_item_ids": True},
                    use_proxy=enabled_proxy,
                    retry_safe=True,
                )
                with self._lock:
                    self._state["api_requests"] += 1
                if not 200 <= response.status_code < 300:
                    detail = short_response(response) or f"HTTP {response.status_code}"
                    raise ValueError(f"Пачка {batch_index}: HTTP {response.status_code}: {detail}")
                try:
                    item_map = extract_bulk_item_map(response.json())
                except ValueError as exc:
                    raise ValueError(f"Пачка {batch_index}: LZT API вернул повреждённый JSON") from exc
                with self._lock:
                    rows_by_source = {str(row["source_id"]): row for row in self._results}
                    for source_id in batch:
                        row = rows_by_source.get(str(source_id))
                        if not row:
                            continue
                        target_id = previous_item_id(source_id, item_map.get(str(source_id), {}))
                        if target_id:
                            row.update({
                                "target_id": str(target_id),
                                "status": "ready",
                                "message": f"Предыдущий ID найден: {target_id}",
                            })
                        else:
                            row.update({
                                "target_id": None,
                                "status": "unresolved",
                                "message": "Предыдущий ID в цепочке не найден",
                            })
                    self._state["processed"] += len(batch)
                    self._state["batches_done"] += 1
                    self._revision += 1
                self._log(f"Same ID · пачка {batch_index}/{len(batches)} готова")
            with self._lock:
                seen_targets: set[str] = set()
                for row in self._results:
                    if row.get("status") != "ready" or not row.get("target_id"):
                        continue
                    target = str(row["target_id"])
                    if target in seen_targets:
                        row.update({
                            "status": "duplicate",
                            "message": "Этот ID уже добавлен другой строкой и не будет отправлен повторно",
                        })
                    else:
                        seen_targets.add(target)
                ready = sum(row.get("status") == "ready" for row in self._results)
                self._state.update({
                    "running": False,
                    "status": "ready" if ready else "error",
                    "phase": None,
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                    "error": None if ready else "Не найдено ни одного предыдущего ID",
                })
                self._revision += 1
            self._log(f"Очередь подготовлена · найдено {ready}/{len(ids)} предыдущих ID", "ok" if ready else "error")
        except InterruptedError:
            with self._lock:
                self._state.update({
                    "running": False,
                    "status": "cancelled",
                    "phase": None,
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                })
                self._revision += 1
            self._log("Получение Same ID остановлено", "warning")
        except (requests.RequestException, TypeError, ValueError) as exc:
            with self._lock:
                self._state.update({
                    "running": False,
                    "status": "error",
                    "phase": None,
                    "error": str(exc),
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                })
                self._revision += 1
            self._log(str(exc), "error")

    def create(self, token: str, retry_errors: bool = False) -> dict[str, Any]:
        token = str(token or "").strip()
        if not token:
            raise ValueError("Выбери LZT-аккаунт с API-токеном")
        with self._lock:
            if self._state.get("running"):
                raise RuntimeError("Дождись завершения текущей операции")
            allowed = {"error"} if retry_errors else {"ready"}
            targets = [str(row["target_id"]) for row in self._results if row.get("status") in allowed and row.get("target_id")]
            targets = list(dict.fromkeys(targets))
            if not targets:
                raise ValueError("Нет аккаунтов для создания арбитражей")
            for row in self._results:
                if str(row.get("target_id") or "") in targets:
                    row.update({"status": "ready", "message": "Ожидает отправки", "http_status": None, "api_response": ""})
            self._cancel.clear()
            self._state.update({
                "running": True,
                "status": "running",
                "phase": "create",
                "error": None,
                "started_at": datetime.now().isoformat(timespec="seconds"),
                "finished_at": None,
                "total": len(targets),
                "processed": 0,
                "total_batches": len(targets),
                "batches_done": 0,
            })
            self._revision += 1
            self._thread = threading.Thread(
                target=self._create_claims,
                args=(token, targets),
                name="utility-mass-claims-create",
                daemon=True,
            )
            self._thread.start()
        return self.status()

    def _create_claims(self, token: str, target_ids: list[str]) -> None:
        try:
            client, _enabled_proxy, cfg = self._client(token)
            interval = int(self._state.get("interval_seconds") or 63)
            claim_cfg = dict(cfg)
            claim_cfg["auto_claim_enabled"] = True
            # Keep the shared claim limiter compatible with the main checker,
            # while the user-selected interval below remains interruptible.
            claim_cfg["claim_min_interval_seconds"] = MIN_CLAIM_INTERVAL_SECONDS
            self._log(f"Создание арбитражей · {len(target_ids)} аккаунтов · интервал {interval} сек")
            for index, target_id in enumerate(target_ids, start=1):
                if self._cancel.is_set():
                    raise InterruptedError
                with self._lock:
                    row = next((item for item in self._results if str(item.get("target_id")) == target_id), None)
                    if row:
                        row.update({"status": "creating", "message": f"Отправка {index}/{len(target_ids)}"})
                    self._revision += 1
                self._log(f"Арбитраж {index}/{len(target_ids)} · #{target_id}")
                try:
                    wait_claim_rate_limit(claim_cfg)
                    if self._cancel.is_set():
                        raise InterruptedError
                    ok, message = post_claim(client, claim_cfg, target_id, self._description)
                    with self._lock:
                        self._state["api_requests"] += 1
                    if ok:
                        mark_claim_sent(claim_cfg)
                        status = "created"
                        display_message = "Арбитраж создан"
                        self._log(f"#{target_id} · арбитраж создан", "ok")
                    else:
                        status = "error"
                        display_message = message
                        self._log(f"#{target_id} · {message}", "error")
                except InterruptedError:
                    raise
                except (requests.RequestException, TypeError, ValueError) as exc:
                    status = "error"
                    display_message = f"{type(exc).__name__}: {exc}"
                    self._log(f"#{target_id} · {display_message}", "error")
                with self._lock:
                    row = next((item for item in self._results if str(item.get("target_id")) == target_id), None)
                    if row:
                        row.update({
                            "status": status,
                            "message": display_message,
                            "api_response": display_message if status == "error" else "",
                            "created_at": datetime.now().isoformat(timespec="seconds") if status == "created" else None,
                        })
                    self._state["processed"] += 1
                    self._state["batches_done"] += 1
                    self._revision += 1
                if index < len(target_ids):
                    if self._cancel.wait(interval):
                        raise InterruptedError
            with self._lock:
                errors = sum(row.get("status") == "error" for row in self._results)
                created = sum(row.get("status") == "created" for row in self._results)
                self._state.update({
                    "running": False,
                    "status": "completed" if not errors else "completed_with_errors",
                    "phase": None,
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                })
                self._revision += 1
            self._log(f"Готово · создано {created} · ошибок {errors}", "ok" if not errors else "warning")
        except InterruptedError:
            with self._lock:
                self._state.update({
                    "running": False,
                    "status": "cancelled",
                    "phase": None,
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                })
                self._revision += 1
            self._log("Создание арбитражей остановлено", "warning")
        except Exception as exc:
            with self._lock:
                self._state.update({
                    "running": False,
                    "status": "error",
                    "phase": None,
                    "error": str(exc),
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                })
                self._revision += 1
            self._log(f"Неожиданная ошибка: {exc}", "error")

    def cancel(self) -> dict[str, Any]:
        self._cancel.set()
        return self.status()

    def clear(self) -> dict[str, Any]:
        with self._lock:
            if self._state.get("running"):
                raise RuntimeError("Сначала останови текущую операцию")
            self._results = []
            self._logs.clear()
            self._description = ""
            self._state = self._empty_state()
            self._revision += 1
        return self.status()

    def stop(self) -> None:
        self._cancel.set()
