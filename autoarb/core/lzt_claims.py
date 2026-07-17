"""
Создание претензии (claim) на LZT Market.
https://lzt-market.readme.io/reference/managingcreateclaim
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

from .lzt_common import api_base
from .error_policy import is_maintenance_error
from .throttled_client import ThrottledClient

_claim_file_lock = threading.Lock()

ReasonCode = Literal["KT", "REC"]


def format_warranty_end_msk(end_unix: int) -> str:
    if not end_unix or end_unix <= 0:
        return "не указана"
    dt = datetime.fromtimestamp(int(end_unix), tz=timezone.utc) + timedelta(hours=3)
    return dt.strftime("%d.%m.%Y %H:%M")


def format_claim_post_body(
    reason: ReasonCode,
    discovery: datetime,
    warranty_end_unix: int,
) -> str:
    """Три строки: причина, дата проверки, дата окончания гарантии (лот в тексте не дублируем)."""
    disc = discovery.strftime("%d.%m.%Y %H:%M")
    wend = format_warranty_end_msk(warranty_end_unix)
    return (
        f"{reason}\n"
        f"Дата проверки и обнаружения: {disc}\n"
        f"Дата окончания гарантии: {wend}"
    )


def _rate_file_path(config: dict) -> Path:
    p = config.get("claim_rate_state_path")
    if p:
        return Path(p)
    from .paths import CLAIM_RATE_FILE

    return CLAIM_RATE_FILE


def wait_claim_rate_limit(config: dict) -> None:
    """Не чаще claim_min_interval_seconds между успешными претензиями (по файлу)."""
    if not config.get("auto_claim_enabled", True):
        return
    interval = max(1, int(config.get("claim_min_interval_seconds", 60)))
    path = _rate_file_path(config)
    with _claim_file_lock:
        now = time.time()
        last = 0.0
        if path.is_file():
            try:
                last = float(path.read_text(encoding="utf-8").strip())
            except ValueError:
                last = 0.0
        need = last + interval - now
        if need > 0:
            time.sleep(need)


def mark_claim_sent(config: dict) -> None:
    path = _rate_file_path(config)
    with _claim_file_lock:
        path.write_text(str(time.time()), encoding="utf-8")


def post_claim(
    client: ThrottledClient,
    config: dict,
    item_id: str,
    post_body: str,
) -> tuple[bool, str]:
    """
    POST /claims (JSON → form → POST /{id}/claims). Каждый вызов client.post уже с паузой.
    """
    base = api_base(config)
    iid = int(item_id)

    r = client.post(f"{base}/claims", json_body={"item_id": iid, "post_body": post_body})
    if r.status_code in (200, 201):
        return True, "claims+json"
    err = _short_err(r)
    if is_maintenance_error(err, r.status_code):
        return False, err

    r2 = client.post(
        f"{base}/claims",
        data={"item_id": str(iid), "post_body": post_body},
    )
    if r2.status_code in (200, 201):
        return True, "claims+form"
    err = _short_err(r2)
    if is_maintenance_error(err, r2.status_code):
        return False, err

    r3 = client.post(f"{base}/{iid}/claims", json_body={"post_body": post_body})
    if r3.status_code in (200, 201):
        return True, "item/claims+json"
    err = _short_err(r3)

    return False, err


def _short_err(r) -> str:
    try:
        body = json.dumps(r.json(), ensure_ascii=False)[:500]
    except json.JSONDecodeError:
        body = (r.text or "")[:500]
    return f"HTTP {r.status_code}: {body}" if body else f"HTTP {r.status_code}"


def try_auto_claim(
    client: ThrottledClient,
    config: dict,
    item_id: str,
    reason: ReasonCode,
    warranty_end_unix: int,
    history_path: Path,
    discovery: datetime | None = None,
) -> tuple[bool, str]:
    """
    Соблюдает интервал, отправляет претензию, пишет историю.
    """
    if not config.get("auto_claim_enabled", True):
        return False, "disabled"

    when = discovery or datetime.now()
    body = format_claim_post_body(reason, when, warranty_end_unix)

    wait_claim_rate_limit(config)
    ok, msg = post_claim(client, config, item_id, body)
    if ok:
        mark_claim_sent(config)

    stamp = when.strftime("%d-%m-%Y %H:%M:%S")
    line = f"{stamp} | item {item_id} | {reason} | {'ok' if ok else 'fail'} | {msg}"
    with history_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

    return ok, msg
