from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

from .paths import TELEGRAM_ERR_LOG
from .throttled_client import ThrottledClient

_TELEGRAM_ERR_LOG = TELEGRAM_ERR_LOG

DEFAULT_NOTIFICATION_TEMPLATE = "{icon} {title}\n\n{message}\n\n🕒 {time} · LZT Control"
DEFAULT_ERROR_TEMPLATE = "🚨 {title}\n\n{message}\n\n🕒 {time} · Требуется внимание"


def _message_category(message: str, category: str | None = None) -> str:
    if category in {"error", "normal"}:
        return category
    value = str(message or "").casefold()
    markers = ("⚠", "❌", "ошиб", "не удалось", "fail", "недоступ", "остановлен")
    return "error" if any(marker in value for marker in markers) else "normal"


def _message_title(message: str, category: str) -> tuple[str, str, str]:
    value = str(message or "").strip()
    lower = value.casefold()
    if category == "error":
        title, icon = "Ошибка LZT Control", "⚠️"
    elif "выставлен на продажу" in lower:
        title, icon = "Аккаунт опубликован", "🛒"
    elif "пролив" in lower:
        title, icon = "Пролив аккаунта", "📤"
    elif "претенз" in lower:
        title, icon = "Претензия", "🛡️"
    elif "кт" in lower:
        title, icon = "Проверка КТ", "🔎"
    elif "валид" in lower or "провер" in lower:
        title, icon = "Проверка аккаунта", "✅"
    else:
        title, icon = "Уведомление", "🔔"
    clean = re.sub(r"^(?:⚠️?|✅|❌|🛒|📤|🔁|🔄|🗑|🛡️?|🔎|🔔)\s*", "", value)
    return title, icon, clean or value


def render_telegram_message(message: str, config: dict, category: str | None = None) -> tuple[str, str]:
    """Apply a consistent user-editable template and return text + category."""
    resolved = _message_category(message, category)
    title, icon, clean = _message_title(message, resolved)
    template_key = "telegram_error_template" if resolved == "error" else "telegram_notification_template"
    fallback = DEFAULT_ERROR_TEMPLATE if resolved == "error" else DEFAULT_NOTIFICATION_TEMPLATE
    template = str(config.get(template_key) or fallback)[:4096]
    values = {
        "{icon}": icon,
        "{title}": title,
        "{message}": clean,
        "{time}": datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
    }
    rendered = template
    for placeholder, value in values.items():
        rendered = rendered.replace(placeholder, value)
    return rendered.strip()[:4096], resolved


def _log_telegram_failure(detail: str) -> None:
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {detail}\n"
    try:
        with _TELEGRAM_ERR_LOG.open("a", encoding="utf-8") as f:
            f.write(line)
    except OSError:
        pass


def _send_telegram_one(
    client: ThrottledClient,
    message: str,
    token: str,
    chat_id,
    *,
    log_prefix: str = "",
) -> bool:
    """Одна отправка; log_prefix — префикс строки в telegram_notify_errors.txt."""
    pre = f"{log_prefix}: " if log_prefix else ""
    if not token:
        _log_telegram_failure(f"{pre}пустой токен бота")
        return False
    if chat_id is None or str(chat_id).strip() == "":
        _log_telegram_failure(f"{pre}пустой chat_id")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    cid = str(chat_id).strip()
    chat_val: str | int = int(cid) if cid.lstrip("-").isdigit() else cid
    has_html = "<a " in message or "<b>" in message or "<i>" in message or "<code>" in message
    payload: dict = {"chat_id": chat_val, "text": message[:4096]}
    if has_html:
        payload["parse_mode"] = "HTML"
    try:
        r = client.post_plain(url, data=payload, use_proxy=False)
    except Exception as e:
        _log_telegram_failure(f"{pre}исключение: {e!r}")
        return False

    try:
        data = r.json()
    except json.JSONDecodeError:
        _log_telegram_failure(f"{pre}HTTP {r.status_code}, не JSON: {(r.text or '')[:400]}")
        return False

    if r.status_code == 200 and data.get("ok") is True:
        return True

    desc = data.get("description") if isinstance(data, dict) else None
    _log_telegram_failure(
        f"{pre}HTTP {r.status_code} ok={data.get('ok') if isinstance(data, dict) else None} "
        f"{desc or json.dumps(data, ensure_ascii=False)[:400]}"
    )
    return False


def send_telegram(
    client: ThrottledClient,
    message: str,
    config: dict,
    *,
    category: str | None = None,
) -> bool:
    """
    Уведомление всем включённым получателям из telegram_bots (до пяти).
    Старые telegram_* поля поддерживаются для конфигов до появления менеджера.
    Возвращает True, если все включённые получатели доставлены успешно.
    """
    formatted, resolved_category = render_telegram_message(message, config, category)
    if isinstance(config.get("telegram_bots"), list):
        bots = config["telegram_bots"][:5]
        selected_error_index = -1
        try:
            selected_error_index = int(config.get("telegram_error_bot_index", -1))
        except (TypeError, ValueError):
            pass
        separate_errors = bool(config.get("telegram_separate_errors", False))
        valid_error_bot = (
            0 <= selected_error_index < len(bots)
            and isinstance(bots[selected_error_index], dict)
            and bots[selected_error_index].get("enabled", True)
        )
        results = []
        for index, row in enumerate(bots):
            if not isinstance(row, dict) or not row.get("enabled", True):
                continue
            if separate_errors and valid_error_bot:
                if resolved_category == "error" and index != selected_error_index:
                    continue
                if resolved_category != "error" and index == selected_error_index:
                    continue
            name = str(row.get("name") or f"bot-{index + 1}").strip()
            results.append(_send_telegram_one(
                client,
                formatted,
                str(row.get("token") or "").strip(),
                row.get("chat_id"),
                log_prefix=name,
            ))
        return all(results)

    main_ok = True
    if config.get("telegram_enabled", True):
        token = str(config.get("telegram_token") or "").strip()
        chat_id = config.get("telegram_chat_id")
        if not token or chat_id is None or str(chat_id).strip() == "":
            _log_telegram_failure("нет telegram_token или telegram_chat_id в config")
            main_ok = False
        else:
            main_ok = _send_telegram_one(client, formatted, token, chat_id)

    friend_ok = True
    fc = config.get("telegram_friend_chat_id")
    if (
        fc is not None
        and str(fc).strip() != ""
        and config.get("telegram_friend_enabled", True)
    ):
        ftok = str(config.get("telegram_friend_token") or config.get("telegram_token") or "").strip()
        friend_ok = _send_telegram_one(
            client, formatted, ftok, fc, log_prefix="friend"
        )

    return main_ok and friend_ok
