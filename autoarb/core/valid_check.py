"""Плановая проверка: LZT check-account → Steam КТ → постановка в очередь пролива (endDate + 1 мин)."""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from pathlib import Path

from rich.console import Console

from .error_policy import classify_retry, deferred_retry_delay_seconds, is_deferred_retry
from .lzt_claims import try_auto_claim
from .lzt_common import api_base
from .lzt_item import check_item_alive, fetch_market_item, guarantee_end_unix, steam_profile_url_from_item
from .notify import send_telegram
from .orders_sync import slot_label
from .paths import CLAIM_HISTORY, PIPELINE_LOG, PROLIV_QUEUE, RESOLD_FILE, VALIDATION_ERRORS
from .proliv import schedule_after_warranty
from .steam_kt import steam_kt_passes
from .storage import (
    append_pipeline_line, append_valid_history,
    dates_file_lock, load_dates, save_resold_item, write_dates,
    valid_errors_file_lock, load_validation_errors, save_validation_errors,
    upsert_validation_error, dismiss_validation_error,
)
from .throttled_client import ThrottledClient

console = Console()

# HTTP-коды / исходы, считающиеся временными (сеть лагает / сервер перегружен)
_TRANSIENT_OUTCOMES = frozenset({"exception", "http_429", "http_503", "http_504", "http_520", "http_521", "http_522", "http_524"})


def _is_transient(outcome: str) -> bool:
    """True если ошибка временная (стоит повторить), False если нужно обрабатывать сразу."""
    return outcome in _TRANSIENT_OUTCOMES or (outcome.startswith("http_5") and outcome != "http_500")


def next_scheduled(dates: dict[str, datetime]) -> tuple[str, datetime] | None:
    if not dates:
        return None
    return min(dates.items(), key=lambda x: x[1])


def check_account_valid(
    client: ThrottledClient,
    item_id: str,
    config: dict,
    *,
    attempt: int = 0,
    quiet: bool = False,
    response_details: dict | None = None,
) -> str:
    base = api_base(config)
    url = f"{base}/{item_id}/check-account"
    outcome = "error"
    try:
        r = client.post(url)
        # Парсим тело ответа вне зависимости от кода — LZT может вернуть
        # "Неверный логин или пароль" как с HTTP 200, так и с HTTP 403.
        try:
            data = r.json()
        except Exception:
            data = {}

        errs = data.get("errors") or []
        errs_str = str(errs)
        if response_details is not None:
            response_details.clear()
            response_details.update({
                "endpoint": f"/{item_id}/check-account",
                "http_status": r.status_code,
                "api_response": json.dumps(data, ensure_ascii=False, separators=(",", ":"))[:4000],
                "message": errs_str[:1000],
            })

        # LZT запускает проверку асинхронно и может попросить забрать результат
        # повторно. Это не ошибка аккаунта и не исчерпание rate limit.
        if is_deferred_retry(data):
            if attempt < 2:
                return check_account_valid(
                    client, item_id, config, attempt=attempt + 1, quiet=quiet,
                    response_details=response_details,
                )
            if response_details is not None:
                response_details["message"] = "LZT обрабатывает проверку; ожидаем готовый результат"
                response_details["retry_delay_seconds"] = deferred_retry_delay_seconds(data)
            if not quiet:
                console.print(f"[blue]{item_id}: проверка ещё выполняется на стороне LZT[/blue]")
            return "deferred_retry"

        # Капча — повторяем попытку (только при 200)
        if r.status_code == 200 and "captcha" in errs_str:
            if attempt < 2:
                return check_account_valid(
                    client, item_id, config, attempt=attempt + 1, quiet=quiet,
                    response_details=response_details,
                )
            if not quiet:
                console.print(f"[red]{item_id}: капча, проверка не прошла[/red]")
            send_telegram(
                client,
                f"Во время проверки https://lzt.market/{item_id} произошла ошибка: проверка на валид не сработала",
                config,
            )
            return "captcha_fail"

        # Валид
        if r.status_code == 200 and data.get("status") == "ok":
            if not quiet:
                console.print(f"[green]{item_id}: валид[/green]")
            send_telegram(client, f"✅ Аккаунт https://lzt.market/{item_id} ВАЛИДНЫЙ", config)
            return "valid"

        # Невалид — «Неверный логин или пароль» может прийти и при 200, и при 403
        if "Неверный логин или пароль у данного аккаунта" in errs_str:
            if not quiet:
                console.print(f"[yellow]{item_id}: не валид (HTTP {r.status_code})[/yellow]")
            send_telegram(
                client,
                f"⛔ Аккаунт <a href=\"https://lzt.market/{item_id}/\">{item_id}</a> НЕ ВАЛИДНЫЙ!!!",
                config,
            )
            return "invalid"

        # Всё остальное — нераспознанный ответ, логируем как http_NNN
        if not quiet:
            console.print(f"[blue]{item_id}: HTTP {r.status_code} — {errs_str[:120]}[/blue]")
        send_telegram(
            client,
            f"⚠️ Проверка <a href=\"https://lzt.market/{item_id}/\">{item_id}</a>: HTTP {r.status_code}",
            config,
        )
        outcome = f"http_{r.status_code}"
    except Exception as e:
        if response_details is not None:
            response_details.clear()
            response_details.update({
                "endpoint": f"/{item_id}/check-account",
                "http_status": None,
                "api_response": str(e)[:4000],
                "message": str(e)[:1000],
            })
        if not quiet:
            console.print(f"[red]{item_id}: {e}[/red]")
        send_telegram(client, f"⚠️ Проверка https://lzt.market/{item_id}: {e}", config)
        outcome = "exception"
    return outcome


def run_valid_due(
    client: ThrottledClient,
    config: dict,
    dates_path: Path,
    history_path: Path,
    *,
    quiet: bool = False,
) -> bool:
    dates = load_dates(dates_path)
    nxt = next_scheduled(dates)
    if not nxt:
        return False
    key, when = nxt
    if datetime.now() < when:
        return False

    # Извлекаем реальный item_id и слот из ключа вида "123456#P30".
    # Старые записи без "#" считаются финальной проверкой.
    if "#" in key:
        item_id, slot = key.split("#", 1)
    else:
        item_id, slot = key, "E"

    # Финальная проверка — если после неё больше нет запланированных точек для этого аккаунта
    remaining = [k for k in dates if k != key and k.startswith(f"{item_id}#")]
    is_final = len(remaining) == 0

    label = slot_label(slot)

    if not quiet:
        console.print(
            f"\n[bold cyan]Проверка [{label}][/bold cyan] "
            f"[link=https://lzt.market/{item_id}/]{item_id}[/link]"
        )

    # finish() объявлена здесь — до любых вызовов, включая пре-чек
    def finish(*, cancel_remaining: bool = False) -> bool:
        # Свежее чтение внутри блокировки — не теряем записи от sync-потока
        with dates_file_lock:
            fresh = load_dates(dates_path)
            if cancel_remaining:
                for k in [k for k in list(fresh) if k == item_id or k.startswith(f"{item_id}#")]:
                    del fresh[k]
            else:
                fresh.pop(key, None)
            write_dates(dates_path, fresh)
        return True

    # Быстрый пре-чек: лот не должен быть удалён или перепродан нами
    alive_state, alive_desc = check_item_alive(client, config, item_id)
    if alive_state == "resold" and isinstance(alive_desc, dict):
        new_id = str(alive_desc.get("new_item_id", "?"))
        old_url = f"https://lzt.market/{item_id}/"
        new_url = f"https://lzt.market/{new_id}/"
        msg = (
            f"🔁 Проверка недоступна — вы отменили гарантию и перепродали аккаунт\n"
            f"Аккаунт: <a href=\"{old_url}\">{item_id}</a>\n"
            f"После перепродажи: <a href=\"{new_url}\">{new_id}</a>"
        )
        if not quiet:
            console.print(f"[yellow]{msg}[/yellow]")
        send_telegram(client, msg, config)
        save_resold_item(RESOLD_FILE, item_id, new_id, datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
        append_valid_history(history_path, item_id, f"resold → {new_id} [{slot}]")
        append_pipeline_line(PIPELINE_LOG, item_id, f"pre_check=resold: new_item_id={new_id}")
        return finish(cancel_remaining=True)

    if alive_state in ("sold", "deleted"):
        icons = {"sold": "🔄", "deleted": "🗑"}
        action_labels = {"sold": "уже продан", "deleted": "удалён"}
        url = f"https://lzt.market/{item_id}/"
        msg = f"{icons[alive_state]} Лот <a href=\"{url}\">{item_id}</a> {action_labels[alive_state]} — пропускаем ({alive_desc})"
        if not quiet:
            color = "yellow" if alive_state == "sold" else "red"
            console.print(f"[{color}]{msg}[/{color}]")
        send_telegram(client, msg, config)
        append_valid_history(history_path, item_id, f"{alive_state} [{slot}]")
        append_pipeline_line(PIPELINE_LOG, item_id, f"pre_check={alive_state}: {alive_desc}")
        return finish(cancel_remaining=True)

    request_details: dict = {}
    item: dict | None = None
    if alive_state == "error":
        outcome = "exception"
        request_details = {
            "endpoint": f"/{item_id}?parse_same_item_ids=true",
            "http_status": None,
            "api_response": str(alive_desc)[:4000],
            "message": str(alive_desc)[:1000],
        }
        append_pipeline_line(PIPELINE_LOG, item_id, f"pre_check=temporary_error: {alive_desc}")
    else:
        outcome = check_account_valid(
            client, item_id, config, quiet=quiet, response_details=request_details,
        )

    # Обе успешные ветки ниже всё равно используют данные лота. Получаем их
    # здесь, чтобы 503/техработы обрабатывались тем же часовым переносом, а не
    # быстрым циклом фонового worker-а.
    if outcome in ("valid", "invalid"):
        try:
            item = fetch_market_item(client, config, item_id)
        except Exception as exc:
            outcome = "exception"
            request_details = {
                "endpoint": f"/{item_id}",
                "http_status": None,
                "api_response": str(exc)[:4000],
                "message": str(exc)[:1000],
            }

    # ``retry_request`` is an internal pending state, not an error.  It does
    # not consume retry attempts, is not saved to the Errors tab and does not
    # send Telegram notifications.
    if outcome == "deferred_retry":
        policy = classify_retry(request_details.get("api_response") or outcome)
        delay_seconds = int(request_details.get("retry_delay_seconds") or policy["delay_seconds"])
        next_retry = datetime.now() + timedelta(seconds=max(15, delay_seconds))
        with dates_file_lock:
            fresh = load_dates(dates_path)
            fresh[key] = next_retry
            write_dates(dates_path, fresh)
        dismiss_validation_error(VALIDATION_ERRORS, valid_errors_file_lock, key)
        append_pipeline_line(
            PIPELINE_LOG, item_id,
            f"check_pending=retry_request next={next_retry.strftime('%H:%M:%S')} [{slot}]",
        )
        return True

    # ── Transient errors: retry before giving up ──────────────────────────────
    # exception / http_5xx / http_429 — сеть или сервер временно недоступен.
    # Обычный временный сбой переносится на 10 минут, а HTTP 503/техработы —
    # сразу на час. После пяти неудачных попыток запись остаётся в «Ошибках».
    if _is_transient(outcome):
        max_retries = max(1, min(5, int(config.get("valid_retry_max", 5))))
        policy = classify_retry(
            request_details.get("api_response") or request_details.get("message") or outcome,
            request_details.get("http_status"),
        )

        with valid_errors_file_lock:
            all_errors = load_validation_errors(VALIDATION_ERRORS)
            existing   = next((e for e in all_errors if e.get("err_key") == key), None)

        retry_count = (existing.get("retry_count", 0) if existing else 0) + 1

        delay_min = max(1, int(policy["delay_seconds"]) // 60)

        entry: dict = {
            "err_key":         key,
            "item_id":         item_id,
            "slot":            slot,
            "slot_label":      label,
            "first_error_at":  existing.get("first_error_at") if existing else datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            "last_error_at":   datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            "retry_count":     retry_count,
            "max_retries":     max_retries,
            "last_error_type": outcome,
            "error_kind":      policy["kind"],
            "error_label":     policy["label"],
            "source":          "validation",
            "stage":           "Проверка на валид",
            "endpoint":        request_details.get("endpoint", f"/{item_id}/check-account"),
            "http_status":     request_details.get("http_status"),
            "error_message":   request_details.get("message") or str(outcome),
            "api_response":    request_details.get("api_response") or str(outcome),
            "exhausted":       retry_count >= max_retries,
        }

        if retry_count < max_retries:
            next_retry = datetime.now() + timedelta(minutes=delay_min)
            entry["next_retry_at"] = next_retry.strftime("%d-%m-%Y %H:%M:%S")

            # Перепланируем (обновляем дату в dates_of_check.txt)
            with dates_file_lock:
                fresh = load_dates(dates_path)
                fresh[key] = next_retry
                write_dates(dates_path, fresh)

            upsert_validation_error(VALIDATION_ERRORS, valid_errors_file_lock, key, entry)

            delay_label = f"{delay_min} мин" + (" [тех. обслуживание]" if policy["kind"] == "maintenance" else "")
            if not quiet:
                console.print(
                    f"[yellow]{item_id}: {outcome} — повтор {retry_count}/{max_retries} "
                    f"через {delay_label}[/yellow]"
                )
            append_pipeline_line(
                PIPELINE_LOG, item_id,
                f"transient_error={outcome} retry={retry_count}/{max_retries} delay={delay_min}m"
            )
            append_valid_history(history_path, item_id, f"retry_{retry_count} [{slot}]")
            return True  # проверка перенесена, finish() не вызываем

        else:
            # Все повторы исчерпаны — записываем «exhausted», убираем из расписания как обычно
            entry["next_retry_at"] = None
            upsert_validation_error(VALIDATION_ERRORS, valid_errors_file_lock, key, entry)
            send_telegram(
                client,
                f"❌ Все {max_retries} попытки проверки завершились ошибкой\n"
                f"https://lzt.market/{item_id}/  ({outcome})",
                config,
            )
            if not quiet:
                console.print(
                    f"[red]{item_id}: все {max_retries} попытки исчерпаны ({outcome})[/red]"
                )
            append_pipeline_line(
                PIPELINE_LOG, item_id,
                f"transient_exhausted={outcome} retries={max_retries}"
            )
            append_valid_history(history_path, item_id, f"error_exhausted [{slot}]")
            return finish(cancel_remaining=False)
    # ── End transient retry ───────────────────────────────────────────────────

    # Если временная ошибка была раньше и теперь всё ок — убираем её.
    if outcome == "valid":
        dismiss_validation_error(VALIDATION_ERRORS, valid_errors_file_lock, key)

    if outcome == "invalid":
        invalid_entry = {
            "err_key": key,
            "item_id": item_id,
            "slot": slot,
            "slot_label": label,
            "source": "validation",
            "stage": "Проверка на валид",
            "first_error_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            "last_error_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            "retry_count": 1,
            "max_retries": 1,
            "last_error_type": "invalid_credentials",
            "error_kind": "invalid_credentials",
            "error_label": "Неверный логин или пароль",
            "error_message": "Аккаунт не прошёл проверку на валид",
            "endpoint": request_details.get("endpoint", f"/{item_id}/check-account"),
            "http_status": request_details.get("http_status"),
            "api_response": request_details.get("api_response", ""),
            "next_retry_at": None,
            "exhausted": True,
        }
        upsert_validation_error(VALIDATION_ERRORS, valid_errors_file_lock, key, invalid_entry)

    append_valid_history(history_path, item_id, f"{outcome} [{slot}]")

    if outcome == "invalid":
        append_pipeline_line(PIPELINE_LOG, item_id, f"valid=FAIL [{label}]")
        if config.get("auto_claim_enabled", True):
            wu = guarantee_end_unix(item or {})
            # Претензию подаём только пока гарантия ещё активна
            if wu > 0 and int(time.time()) > wu:
                notify_msg = (
                    f"⏰ Лот {item_id} невалид, но гарантия уже истекла "
                    f"— претензия не подаётся\nhttps://lzt.market/{item_id}/"
                )
                if not quiet:
                    console.print(f"[yellow]{item_id}: гарантия истекла, претензия пропущена[/yellow]")
                send_telegram(client, notify_msg, config)
                append_pipeline_line(PIPELINE_LOG, item_id, "claim_REC skip: warranty_expired")
            else:
                try:
                    ok_c, cmsg = try_auto_claim(client, config, item_id, "REC", wu, CLAIM_HISTORY)
                except Exception as exc:
                    ok_c, cmsg = False, str(exc)
                claim_key = f"arbitr#{item_id}#REC"
                if ok_c:
                    dismiss_validation_error(VALIDATION_ERRORS, valid_errors_file_lock, claim_key)
                else:
                    claim_policy = classify_retry(cmsg)
                    upsert_validation_error(VALIDATION_ERRORS, valid_errors_file_lock, claim_key, {
                        "err_key": claim_key, "item_id": item_id, "slot": "REC",
                        "slot_label": "Арбитраж REC", "source": "arbitr",
                        "stage": "Создание претензии REC",
                        "first_error_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                        "last_error_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                        "retry_count": 1, "max_retries": 1, "exhausted": True,
                        "last_error_type": "claim_failed", "error_kind": claim_policy["kind"],
                        "error_label": claim_policy["label"] if claim_policy["kind"] == "maintenance" else "Претензия не создана", "error_message": str(cmsg)[:1000],
                        "endpoint": f"/{item_id}/claims", "http_status": claim_policy.get("http_status"),
                        "api_response": str(cmsg)[:4000], "next_retry_at": None,
                    })
                send_telegram(
                    client,
                    (
                        f"📋 Арбитраж (REC): претензия создана\nhttps://lzt.market/{item_id}/"
                        if ok_c
                        else (
                            f"⚠️ Арбитраж (REC): претензия не создана\n"
                            f"https://lzt.market/{item_id}/\n{cmsg}"
                        )
                    ),
                    config,
                )
                append_pipeline_line(
                    PIPELINE_LOG, item_id, f"claim_REC {'ok' if ok_c else 'fail'} {cmsg}"
                )
        return finish(cancel_remaining=True)

    if outcome != "valid":
        # Ошибка (captcha, HTTP и т.п.) — убираем текущую точку, остальные остаются
        now_label = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        upsert_validation_error(VALIDATION_ERRORS, valid_errors_file_lock, key, {
            "err_key": key, "item_id": item_id, "slot": slot,
            "slot_label": label, "source": "validation",
            "stage": "Проверка на валид", "first_error_at": now_label,
            "last_error_at": now_label, "retry_count": 1, "max_retries": 1,
            "exhausted": True, "last_error_type": outcome,
            "error_kind": outcome, "error_label": "Ошибка проверки на валид",
            "error_message": request_details.get("message") or str(outcome),
            "endpoint": request_details.get("endpoint", f"/{item_id}/check-account"),
            "http_status": request_details.get("http_status"),
            "api_response": request_details.get("api_response") or str(outcome),
            "next_retry_at": None,
        })
        append_pipeline_line(PIPELINE_LOG, item_id, f"valid=error {outcome} [{label}]")
        return finish(cancel_remaining=False)

    steam_key = str(config.get("steam_web_api_key") or "").strip()
    kt_enabled = bool(config.get("kt_enabled", True))
    kt_ok = True
    kt_reason = ""

    if kt_enabled:
        steam_url = steam_profile_url_from_item(item or {})
        require_steam = bool(config.get("kt_require_steam_link", True))
        if not steam_url:
            if require_steam:
                kt_ok = False
                kt_reason = "нет Steam в accountLinks"
            else:
                kt_reason = "steam не требуется"
        else:
            kt_ok, kt_reason = steam_kt_passes(steam_key, steam_url)
    else:
        kt_reason = "kt выключен"

    if not kt_ok:
        if not quiet:
            console.print(f"[red]{item_id}: КТ — проблема: {kt_reason}[/red]")
        send_telegram(
            client,
            f"🛑 КТ https://lzt.market/{item_id}/ — {kt_reason}",
            config,
        )
        append_pipeline_line(PIPELINE_LOG, item_id, f"valid=ok kt=FAIL {kt_reason} [{label}]")
        if config.get("auto_claim_enabled", True):
            end_u = guarantee_end_unix(item or {})
            # Претензию подаём только пока гарантия ещё активна
            if end_u > 0 and int(time.time()) > end_u:
                notify_msg = (
                    f"⏰ Лот {item_id} KT-fail, но гарантия уже истекла "
                    f"— претензия не подаётся\nhttps://lzt.market/{item_id}/"
                )
                if not quiet:
                    console.print(f"[yellow]{item_id}: гарантия истекла, претензия пропущена[/yellow]")
                send_telegram(client, notify_msg, config)
                append_pipeline_line(PIPELINE_LOG, item_id, "claim_KT skip: warranty_expired")
            else:
                try:
                    ok_c, cmsg = try_auto_claim(client, config, item_id, "KT", end_u, CLAIM_HISTORY)
                except Exception as exc:
                    ok_c, cmsg = False, str(exc)
                claim_key = f"arbitr#{item_id}#KT"
                if ok_c:
                    dismiss_validation_error(VALIDATION_ERRORS, valid_errors_file_lock, claim_key)
                else:
                    claim_policy = classify_retry(cmsg)
                    upsert_validation_error(VALIDATION_ERRORS, valid_errors_file_lock, claim_key, {
                        "err_key": claim_key, "item_id": item_id, "slot": "KT",
                        "slot_label": "Арбитраж КТ", "source": "arbitr",
                        "stage": "Создание претензии КТ",
                        "first_error_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                        "last_error_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                        "retry_count": 1, "max_retries": 1, "exhausted": True,
                        "last_error_type": "claim_failed", "error_kind": claim_policy["kind"],
                        "error_label": claim_policy["label"] if claim_policy["kind"] == "maintenance" else "Претензия не создана", "error_message": str(cmsg)[:1000],
                        "endpoint": f"/{item_id}/claims", "http_status": claim_policy.get("http_status"),
                        "api_response": str(cmsg)[:4000], "next_retry_at": None,
                    })
                send_telegram(
                    client,
                    (
                        f"📋 Арбитраж (КТ): претензия создана\nhttps://lzt.market/{item_id}/"
                        if ok_c
                        else (
                            f"⚠️ Арбитраж (КТ): претензия не создана\n"
                            f"https://lzt.market/{item_id}/\n{cmsg}"
                        )
                    ),
                    config,
                )
                append_pipeline_line(
                    PIPELINE_LOG, item_id, f"claim_KT {'ok' if ok_c else 'fail'} {cmsg}"
                )
        return finish(cancel_remaining=True)

    # Валид + КТ ок
    if not quiet:
        console.print(f"[green]{item_id}: КТ — ок[/green] ({kt_reason or 'чисто'})")
    if kt_enabled:
        send_telegram(
            client,
            f"✅ КТ пройден [{label}]\nhttps://lzt.market/{item_id}/\n{kt_reason or 'чисто'}",
            config,
        )
    else:
        send_telegram(
            client,
            f"✅ Валид [{label}]; КТ не запускался (kt_enabled=false)\nhttps://lzt.market/{item_id}/",
            config,
        )
    append_pipeline_line(PIPELINE_LOG, item_id, f"valid=ok kt=ok {kt_reason} [{label}]")

    # Пролив планируем только на финальной проверке (слот E)
    if is_final:
        end_unix = guarantee_end_unix(item or {})
        proliv_extra = int(config.get("proliv_after_warranty_seconds", 60))
        if config.get("proliv_enabled", True) and end_unix > 0:
            run_at = schedule_after_warranty(PROLIV_QUEUE, item_id, end_unix, proliv_extra)
            send_telegram(
                client,
                f"📅 Пролив https://lzt.market/{item_id}/ запланирован (Unix {run_at}, +{proliv_extra}s после endDate).",
                config,
            )
            append_pipeline_line(PIPELINE_LOG, item_id, f"proliv_scheduled run_at_unix={run_at}")
        elif config.get("proliv_enabled", True):
            if not quiet:
                console.print(f"[yellow]{item_id}: нет endDate гарантии — пролив не запланирован[/yellow]")
            append_pipeline_line(PIPELINE_LOG, item_id, "proliv_skip no_endDate")

    # Промежуточная проверка пройдена — убираем только эту точку, остальные остаются
    return finish(cancel_remaining=False)
