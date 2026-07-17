from __future__ import annotations

import json
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import requests

from .error_policy import classify_retry
from .lzt_common import api_base
from .lzt_item import check_item_alive, extract_login_password_string, fetch_market_item, fetch_proliv_source
from .notify import send_telegram
from .publishing import fast_sell_publish, goods_check_publish, item_add_resell, post_item_tag
from .paths import GUARANTEE_TXT, RESOLD_FILE
from .storage import (
    append_proliv_history,
    dismiss_validation_error,
    load_proliv_queue,
    proliv_file_lock,
    remove_reference_item,
    save_proliv_queue,
    save_resold_item,
    upsert_validation_error,
    valid_errors_file_lock,
)
from .throttled_client import ThrottledClient


def schedule_after_warranty(
    queue_path: Path,
    item_id: str,
    guarantee_end_unix: int,
    extra_seconds: int = 60,
) -> int:
    """Планирует пролив на endDate + extra_seconds (по умолчанию +1 мин). Возвращает run_at_unix."""
    run_at = int(guarantee_end_unix) + int(extra_seconds)
    with proliv_file_lock:
        rows = load_proliv_queue(queue_path)
        rows = [r for r in rows if str(r.get("item_id")) != str(item_id)]
        rows.append({"item_id": str(item_id), "run_at_unix": run_at})
        rows.sort(key=lambda r: int(r["run_at_unix"]))
        save_proliv_queue(queue_path, rows)
    return run_at


def remove_from_queue(queue_path: Path, item_id: str) -> None:
    with proliv_file_lock:
        rows = load_proliv_queue(queue_path)
        rows = [r for r in rows if str(r.get("item_id")) != str(item_id)]
        save_proliv_queue(queue_path, rows)


def update_queue_item(queue_path: Path, item_id: str, **fields: object) -> bool:
    """Atomically update a live queue row without resurrecting deleted work."""
    with proliv_file_lock:
        rows = load_proliv_queue(queue_path)
        for row in rows:
            if str(row.get("item_id")) == str(item_id):
                row.update(fields)
                save_proliv_queue(queue_path, rows)
                return True
    return False


def next_proliv_scheduled(queue_path: Path) -> tuple[str, int] | None:
    with proliv_file_lock:
        rows = [
            row
            for row in load_proliv_queue(queue_path)
            if not row.get("manual_review")
        ]
    if not rows:
        return None
    # An overdue row is the actual next piece of work.  Previously a later
    # future row hid it from the status API, while the worker processed another
    # item, making the interface look stuck.
    nxt = min(rows, key=lambda r: int(r.get("run_at_unix", 0)))
    return str(nxt["item_id"]), int(nxt["run_at_unix"])


def _proliv_retry_limit(config: dict) -> int:
    try:
        value = int(config.get("proliv_retry_max", 5))
    except (TypeError, ValueError):
        value = 5
    return max(1, min(5, value))


def configured_post_publish_tag(config: dict) -> int | None:
    if not config.get("proliv_after_publish_tag_enabled", True):
        return None
    try:
        tag_id = int(config.get("proliv_after_publish_tag_id", 23))
    except (TypeError, ValueError):
        tag_id = 23
    return tag_id if tag_id > 0 else None


def _consecutive_failures(history_path: Path, item_id: str) -> int:
    """Count the current failure streak, including logs from older versions."""
    if not history_path.exists():
        return 0
    count = 0
    needle = f"| item {item_id} |"
    with history_path.open(encoding="utf-8", errors="replace") as handle:
        for raw in handle:
            line = raw.strip()
            if needle not in line:
                continue
            outcome = line.split(needle, 1)[1].strip().casefold()
            if outcome.startswith("fail"):
                count += 1
            elif outcome.startswith(("ok", "skip", "retry_reset")) or "published" in outcome:
                count = 0
    return count


def _clean_api_response(value: object) -> str:
    """Keep the raw JSON response separate from a human-readable prefix."""
    text = str(value or "").strip()
    for marker in ("{", "["):
        start = text.find(marker)
        if start < 0:
            continue
        candidate = text[start:]
        try:
            parsed = json.loads(candidate)
        except (json.JSONDecodeError, TypeError, ValueError):
            continue
        return json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))[:4000]
    return text[:4000]


def _deferred_retry_fields(run_at_unix: int) -> dict[str, object]:
    return {
        "run_at_unix": int(run_at_unix),
        "retry_count": 0,
        "max_retries": 0,
        "last_error": "",
        "last_error_type": "",
        "last_error_label": "",
        "last_error_at_unix": 0,
        "api_response": "",
        "endpoint": "",
        "http_status": None,
        "manual_review": False,
        "blocked_reason": "",
        "pending_state": "retry_request",
    }


def normalize_deferred_proliv_rows(queue_path: Path) -> int:
    """Migrate previously stored retry_request rows back into the live queue."""
    changed = 0
    now = int(time.time())
    with proliv_file_lock:
        rows = load_proliv_queue(queue_path)
        for row in rows:
            raw = f"{row.get('last_error', '')}\n{row.get('api_response', '')}"
            policy = classify_retry(raw, row.get("http_status"))
            if policy["kind"] != "deferred_retry":
                continue
            row.update(_deferred_retry_fields(now + int(policy["delay_seconds"])))
            changed += 1
        if changed:
            save_proliv_queue(queue_path, rows)
    return changed


def schedule_deferred_proliv(
    queue_path: Path,
    history_path: Path,
    item_id: str,
    detail: str,
) -> int:
    """Quietly reschedule an asynchronous LZT check without creating an error."""
    policy = classify_retry(detail)
    delay_seconds = max(15, int(policy["delay_seconds"]))
    run_at = int(time.time()) + delay_seconds
    update_queue_item(queue_path, item_id, **_deferred_retry_fields(run_at))
    append_proliv_history(
        history_path,
        item_id,
        f"retry_reset: LZT обрабатывает goods/check; повтор через {delay_seconds} сек",
    )
    return delay_seconds


def record_proliv_failure(
    client: ThrottledClient,
    config: dict,
    queue_path: Path,
    history_path: Path,
    row: dict,
    stage: str,
    detail: str,
    *,
    retry_delay_seconds: int | None = None,
    stop_immediately: bool | None = None,
    api_response: str = "",
    endpoint: str = "",
    http_status: int | None = None,
) -> tuple[int, bool]:
    """Persist one failed publication attempt and stop at the configured cap."""
    item_id = str(row.get("item_id") or "")
    max_attempts = _proliv_retry_limit(config)
    policy = classify_retry(detail, http_status)
    if retry_delay_seconds is None:
        retry_delay_seconds = int(policy["delay_seconds"])
    if stop_immediately is None:
        stop_immediately = bool(policy["stop_immediately"])
    http_status = http_status or policy.get("http_status")
    try:
        stored_attempts = max(0, int(row.get("retry_count", 0)))
    except (TypeError, ValueError):
        stored_attempts = 0
    attempts = min(max_attempts, max(stored_attempts, _consecutive_failures(history_path, item_id)) + 1)
    exhausted = stop_immediately or attempts >= max_attempts
    user_detail = str(policy.get("message") or detail).strip()
    reason = f"{stage}: {user_detail}".strip()[:1000]
    clean_response = _clean_api_response(api_response or detail)
    fields = {
        "retry_count": attempts,
        "max_retries": max_attempts,
        "last_error": reason,
        "last_error_type": str(policy["kind"]),
        "last_error_label": str(policy["label"]),
        "last_error_at_unix": int(time.time()),
        "api_response": clean_response,
        "endpoint": str(endpoint or "")[:500],
        "http_status": http_status,
        "retry_delay_seconds": max(0, int(retry_delay_seconds)),
        "manual_review": exhausted,
        "blocked_reason": reason if exhausted else "",
    }
    if not exhausted:
        fields["run_at_unix"] = int(time.time()) + max(30, int(retry_delay_seconds))
    update_queue_item(queue_path, item_id, **fields)
    if exhausted:
        remove_reference_item(GUARANTEE_TXT, item_id)
    state = "автоматический пролив отменён — ошибка добавлена во вкладку «Ошибки»" if exhausted else f"следующая попытка через {max(1, retry_delay_seconds // 60)} мин"
    append_proliv_history(
        history_path,
        item_id,
        f"fail {stage} [{attempts}/{max_attempts}]: {user_detail}",
    )
    send_telegram(
        client,
        (
            f"Аккаунт: https://lzt.market/{item_id}/\n"
            f"Этап: {stage}\n"
            f"Тип: {policy['label']}\n"
            f"Причина: {user_detail[:700]}\n"
            f"Попытка: {attempts}/{max_attempts}\n"
            f"Статус: {state}"
        ),
        config,
        category="error",
    )
    return attempts, exhausted


def _finish_proliv(queue_path: Path, item_id: str) -> None:
    remove_from_queue(queue_path, item_id)
    remove_reference_item(GUARANTEE_TXT, item_id)


def _has_active_guarantee(source_item: dict) -> bool:
    """Return True only while the purchased lot's guarantee is still active."""
    guarantee = source_item.get("guarantee") or {}
    if not isinstance(guarantee, dict) or not guarantee:
        return False
    if bool(guarantee.get("cancelled")):
        return False
    if str(guarantee.get("cancelledReason") or "").strip():
        return False
    try:
        end_unix = int(guarantee.get("endDate") or 0)
    except (TypeError, ValueError):
        end_unix = 0
    if end_unix and end_unix <= int(time.time()):
        return False
    if "active" in guarantee:
        return bool(guarantee.get("active"))
    return end_unix > int(time.time())


def _refuse_guarantee_before_manual_proliv(
    client: ThrottledClient,
    config: dict,
    item_id: str,
) -> tuple[bool, str, int | None, str]:
    """Cancel an active guarantee once and reconcile an ambiguous response."""
    url = f"https://prod-api.lzt.market/{item_id}/refuse-guarantee"
    response = None
    try:
        response = client.post(url)
        try:
            data = response.json()
        except (ValueError, TypeError):
            data = {}
        raw = (
            json.dumps(data, ensure_ascii=False, separators=(",", ":"))[:4000]
            if isinstance(data, dict) else str(response.text or "")[:4000]
        )
        errors = data.get("errors") if isinstance(data, dict) else None
        success = 200 <= response.status_code < 300 and not errors
        if success:
            return True, "Гарантия отменена перед ручным проливом", response.status_code, raw
        detail = str(errors or (data.get("message") if isinstance(data, dict) else "") or f"HTTP {response.status_code}")
    except Exception as exc:
        detail = str(exc)
        raw = str(exc)[:4000]

    # The POST may have reached LZT even if its response was lost. One GET is
    # safer than repeating a mutating request and also handles an already
    # expired/refused guarantee.
    state, description, refreshed = fetch_proliv_source(client, config, item_id)
    if state == "ok" and refreshed and not _has_active_guarantee(refreshed):
        return True, "Гарантия уже отменена или успела закончиться", getattr(response, "status_code", None), raw
    return False, detail, getattr(response, "status_code", None), raw


def _run_proliv_due_legacy(
    client: ThrottledClient,
    config: dict,
    queue_path: Path,
    history_path: Path,
    errors_path: Path | None = None,
) -> bool:
    if not config.get("proliv_enabled", True):
        return False
    # Work on a snapshot.  Every later mutation is merged under the file lock,
    # so a slow HTTP request cannot overwrite another worker/UI action.
    with proliv_file_lock:
        rows = load_proliv_queue(queue_path)
    now = int(time.time())
    for row in rows:
        run_at = int(row.get("run_at_unix", 0))
        if run_at > now:
            continue
        if row.get("manual_review"):
            continue
        source_id = str(row["item_id"])
        draft = row.get("draft_item_id")
        base = api_base(config)

        # Пре-чек: лот не должен быть перепродан, удалён или продан
        alive_state, alive_desc = check_item_alive(client, config, source_id)
        if alive_state == "error":
            record_proliv_failure(
                client, config, queue_path, history_path, row,
                "проверка состояния лота", str(alive_desc),
                api_response=str(alive_desc),
                endpoint=f"/{source_id}?parse_same_item_ids=true",
            )
            return True
        if alive_state == "resold" and isinstance(alive_desc, dict):
            new_id = str(alive_desc.get("new_item_id", "?"))
            old_url = f"https://lzt.market/{source_id}/"
            new_url = f"https://lzt.market/{new_id}/"
            msg = (
                f"🔁 Пролив недоступен — вы отменили гарантию и перепродали аккаунт\n"
                f"Аккаунт: <a href=\"{old_url}\">{source_id}</a>\n"
                f"После перепродажи: <a href=\"{new_url}\">{new_id}</a>"
            )
            append_proliv_history(history_path, source_id, f"skip: resold → {new_id}")
            send_telegram(client, msg, config)
            save_resold_item(RESOLD_FILE, source_id, new_id, datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
            _finish_proliv(queue_path, source_id)
            return True

        if alive_state in ("sold", "deleted"):
            icons = {"sold": "🔄", "deleted": "🗑"}
            labels = {"sold": "уже продан", "deleted": "удалён"}
            url = f"https://lzt.market/{source_id}/"
            msg = f"{icons[alive_state]} Пролив: лот <a href=\"{url}\">{source_id}</a> {labels[alive_state]} — пропускаем ({alive_desc})"
            append_proliv_history(history_path, source_id, f"skip: {alive_state} — {alive_desc}")
            send_telegram(client, msg, config)
            _finish_proliv(queue_path, source_id)
            return True

        try:
            src_item = fetch_market_item(client, config, source_id)
        except Exception as exc:
            record_proliv_failure(
                client, config, queue_path, history_path, row,
                "загрузка исходного лота", str(exc),
                api_response=str(exc), endpoint=f"/{source_id}",
            )
            return True

        if not draft:
            if not src_item:
                record_proliv_failure(
                    client, config, queue_path, history_path, row,
                    "загрузка исходного лота", "LZT API не вернул данные аккаунта",
                )
                return True
            try:
                new_id, add_err = item_add_resell(client, config, src_item, source_id)
            except (requests.ConnectionError, requests.Timeout) as exc:
                new_id, add_err = None, f"uncertain: соединение оборвалось после item/add ({exc})"
            if new_id is None:
                if str(add_err).startswith("uncertain:"):
                    record_proliv_failure(
                        client, config, queue_path, history_path, row,
                        "создание черновика", str(add_err), stop_immediately=True,
                    )
                    return True
                record_proliv_failure(
                    client, config, queue_path, history_path, row,
                    "создание черновика", str(add_err),
                )
                return True
            update_queue_item(queue_path, source_id, draft_item_id=str(new_id))
            draft = str(new_id)

        if not src_item:
            try:
                src_item = fetch_market_item(client, config, source_id)
            except Exception as exc:
                record_proliv_failure(
                    client, config, queue_path, history_path, row,
                    "подготовка публикации", str(exc),
                    api_response=str(exc), endpoint=f"/{source_id}",
                )
                return True
        if not src_item:
            record_proliv_failure(
                client, config, queue_path, history_path, row,
                "подготовка публикации", "нет данных исходного лота для goods/check",
            )
            return True

        if not extract_login_password_string(src_item):
            record_proliv_failure(
                client, config, queue_path, history_path, row,
                "подготовка данных", "в ответе отсутствует login_password",
            )
            return True

        if config.get("proliv_prefetch_goods_add", True):
            client.get(f"{base}/{draft}/goods/add?resell_item_id={source_id}")

        try:
            ok, msg = goods_check_publish(client, config, draft, source_id, src_item)
        except Exception as exc:
            record_proliv_failure(
                client, config, queue_path, history_path, row,
                f"публикация черновика {draft}", str(exc),
                api_response=str(exc), endpoint=f"/{draft}/goods/check",
            )
            return True
        if ok:
            _finish_proliv(queue_path, source_id)
            append_proliv_history(
                history_path,
                source_id,
                f"ok: draft {draft} published ({msg})",
            )
            tag_id = configured_post_publish_tag(config)
            if tag_id is not None:
                try:
                    tag_ok, tag_err = post_item_tag(client, config, draft, tag_id)
                except Exception as exc:
                    tag_ok, tag_err = False, str(exc)
                tag_error_key = f"proliv-tag#{source_id}"
                if not tag_ok:
                    append_proliv_history(
                        history_path,
                        source_id,
                        f"tag {tag_id} fail: {tag_err}",
                    )
                    if errors_path is not None:
                        tag_policy = classify_retry(tag_err)
                        now_label = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                        upsert_validation_error(errors_path, valid_errors_file_lock, tag_error_key, {
                            "err_key": tag_error_key, "item_id": source_id,
                            "slot": "tag", "slot_label": "Метка",
                            "source": "proliv_action", "stage": "Метка после публикации",
                            "first_error_at": now_label, "last_error_at": now_label,
                            "retry_count": 1, "max_retries": 1, "exhausted": True,
                            "last_error_type": "tag_failed", "error_kind": tag_policy["kind"],
                            "error_label": "Метка не добавлена", "error_message": str(tag_err)[:1000],
                            "endpoint": f"/{draft}/tag", "http_status": tag_policy.get("http_status"),
                            "api_response": str(tag_err)[:4000], "next_retry_at": None,
                        })
                elif errors_path is not None:
                    dismiss_validation_error(errors_path, valid_errors_file_lock, tag_error_key)
            listing_url = f"https://lzt.market/{draft}/"
            send_telegram(
                client,
                (
                    f"🛒 Аккаунт выставлен на продажу\n{listing_url}\n"
                    f"Покупка (исходный лот): https://lzt.market/{source_id}/\n"
                    f"({msg})"
                ),
                config,
            )
            return True
        policy = classify_retry(msg)
        if policy["kind"] == "deferred_retry":
            schedule_deferred_proliv(queue_path, history_path, source_id, str(msg))
            if errors_path is not None:
                dismiss_validation_error(
                    errors_path, valid_errors_file_lock, f"proliv#{source_id}",
                )
            return True
        record_proliv_failure(
            client, config, queue_path, history_path, row,
            f"публикация черновика {draft}", str(msg),
            retry_delay_seconds=int(policy["delay_seconds"]),
            stop_immediately=bool(policy["stop_immediately"]),
            api_response=str(msg),
            endpoint=f"/{draft}/goods/check",
            http_status=policy.get("http_status"),
        )
        return True
    return False


def _finish_fast_sell_success(
    client: ThrottledClient,
    config: dict,
    queue_path: Path,
    history_path: Path,
    errors_path: Path | None,
    source_id: str,
    new_item_id: int | None,
    message: str,
) -> None:
    """Persist a confirmed fast-sell result and run optional post-actions."""
    _finish_proliv(queue_path, source_id)
    save_resold_item(
        RESOLD_FILE,
        source_id,
        str(new_item_id) if new_item_id else "",
        datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
    )
    if errors_path is not None:
        dismiss_validation_error(
            errors_path, valid_errors_file_lock, f"proliv#{source_id}",
        )
    target = str(new_item_id) if new_item_id else "unknown"
    append_proliv_history(
        history_path,
        source_id,
        f"ok: fast-sell published {target} ({message})",
    )

    tag_note = ""
    tag_id = configured_post_publish_tag(config)
    if tag_id is not None and new_item_id:
        try:
            tag_ok, tag_error = post_item_tag(client, config, str(new_item_id), tag_id)
        except Exception as exc:
            tag_ok, tag_error = False, str(exc)
        tag_error_key = f"proliv-tag#{source_id}"
        if tag_ok:
            if errors_path is not None:
                dismiss_validation_error(
                    errors_path, valid_errors_file_lock, tag_error_key,
                )
            tag_note = f"\nМетка {tag_id}: добавлена"
        else:
            append_proliv_history(
                history_path, source_id, f"tag {tag_id} fail: {tag_error}",
            )
            tag_note = f"\nМетка {tag_id}: не добавлена ({tag_error})"
            if errors_path is not None:
                tag_policy = classify_retry(tag_error)
                now_label = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                upsert_validation_error(errors_path, valid_errors_file_lock, tag_error_key, {
                    "err_key": tag_error_key, "item_id": source_id,
                    "slot": "tag", "slot_label": "Метка",
                    "source": "proliv_action", "stage": "Метка после публикации",
                    "first_error_at": now_label, "last_error_at": now_label,
                    "retry_count": 1, "max_retries": 1, "exhausted": True,
                    "last_error_type": "tag_failed", "error_kind": tag_policy["kind"],
                    "error_label": "Метка не добавлена", "error_message": str(tag_error)[:1000],
                    "endpoint": f"/{new_item_id}/tag", "http_status": tag_policy.get("http_status"),
                    "api_response": str(tag_error)[:4000], "next_retry_at": None,
                })
    elif tag_id is not None:
        tag_note = f"\nМетка {tag_id}: пропущена — fast-sell не вернул новый item_id"

    listing = (
        f"https://lzt.market/{new_item_id}/"
        if new_item_id else
        "Опубликовано, но LZT API не вернул ID нового объявления"
    )
    send_telegram(
        client,
        (
            f"🛒 Аккаунт выставлен на продажу через fast-sell\n{listing}\n"
            f"Покупка: https://lzt.market/{source_id}/\n"
            f"{message}{tag_note}"
        ),
        config,
    )


def run_proliv_due(
    client: ThrottledClient,
    config: dict,
    queue_path: Path,
    history_path: Path,
    errors_path: Path | None = None,
    on_fast_sell_attempt: Callable[[int, int, str], None] | None = None,
) -> bool:
    """Publish the next due resale through the one-step fast-sell endpoint."""
    if not config.get("proliv_enabled", True):
        return False
    with proliv_file_lock:
        rows = load_proliv_queue(queue_path)
    now = int(time.time())
    for row in rows:
        if int(row.get("run_at_unix", 0)) > now or row.get("manual_review"):
            continue
        source_id = str(row["item_id"])

        state, description, source_item = fetch_proliv_source(
            client, config, source_id,
        )
        if state == "resold" and isinstance(description, dict):
            new_id = str(description.get("new_item_id", "?"))
            append_proliv_history(history_path, source_id, f"skip: resold → {new_id}")
            save_resold_item(
                RESOLD_FILE, source_id, new_id,
                datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            )
            send_telegram(
                client,
                (
                    "🔁 Пролив не запущен — аккаунт уже перепродан\n"
                    f"Покупка: https://lzt.market/{source_id}/\n"
                    f"Перепродажа: https://lzt.market/{new_id}/"
                ),
                config,
            )
            _finish_proliv(queue_path, source_id)
            return True
        if state in {"sold", "deleted"}:
            append_proliv_history(
                history_path, source_id, f"skip: {state} — {description}",
            )
            _finish_proliv(queue_path, source_id)
            return True
        if state == "error" or not source_item:
            record_proliv_failure(
                client, config, queue_path, history_path, row,
                "загрузка исходного лота для fast-sell", str(description),
                api_response=str(description),
                endpoint=f"/{source_id}?parse_same_item_ids=true",
            )
            return True
        if not extract_login_password_string(source_item):
            record_proliv_failure(
                client, config, queue_path, history_path, row,
                "подготовка fast-sell", "в ответе отсутствует login_password",
                stop_immediately=True,
                endpoint=f"/{source_id}?parse_same_item_ids=true",
            )
            return True

        if (
            bool(row.get("manual_requested"))
            and not bool(row.get("manual_guarantee_refused"))
            and _has_active_guarantee(source_item)
        ):
            refused, refuse_message, refuse_status, refuse_response = (
                _refuse_guarantee_before_manual_proliv(
                    client, config, source_id,
                )
            )
            if not refused:
                policy = classify_retry(refuse_message, refuse_status)
                record_proliv_failure(
                    client, config, queue_path, history_path, row,
                    "отмена гарантии перед ручным проливом", refuse_message,
                    retry_delay_seconds=int(policy["delay_seconds"]),
                    stop_immediately=bool(policy["stop_immediately"]),
                    api_response=refuse_response,
                    endpoint=f"/{source_id}/refuse-guarantee",
                    http_status=refuse_status,
                )
                return True
            append_proliv_history(
                history_path, source_id, f"ok manual refuse-guarantee: {refuse_message}",
            )
            row["manual_guarantee_refused"] = True
            update_queue_item(
                queue_path, source_id, manual_guarantee_refused=True,
            )

        try:
            published, message, new_item_id = fast_sell_publish(
                client, config, source_id, source_item,
                on_attempt=on_fast_sell_attempt,
            )
        except Exception as exc:
            record_proliv_failure(
                client, config, queue_path, history_path, row,
                "быстрый пролив fast-sell", str(exc),
                stop_immediately=isinstance(exc, (requests.ConnectionError, requests.Timeout)),
                api_response=str(exc), endpoint="/item/fast-sell",
            )
            return True
        if published:
            _finish_fast_sell_success(
                client, config, queue_path, history_path, errors_path,
                source_id, new_item_id, message,
            )
            return True

        policy = classify_retry(message)
        exhausted = "исчерпаны официальные" in str(message).casefold()
        uncertain = str(message).casefold().startswith("uncertain:")
        record_proliv_failure(
            client, config, queue_path, history_path, row,
            "быстрый пролив fast-sell", str(message),
            retry_delay_seconds=int(policy["delay_seconds"]),
            stop_immediately=bool(policy["stop_immediately"] or exhausted or uncertain),
            api_response=str(message), endpoint="/item/fast-sell",
            http_status=policy.get("http_status"),
        )
        return True
    return False
