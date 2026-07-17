from __future__ import annotations

import json
import re
from pathlib import Path

from .lzt_common import api_base
from .throttled_client import ThrottledClient


def transfer_item(
    client: ThrottledClient,
    config: dict,
    item_id: str,
    username: str,
) -> tuple[bool, str]:
    """POST /{item_id}/change-owner?username=...&secret_answer=..."""
    base = api_base(config)
    secret = str(config.get("transfer_secret_answer", "")).strip()
    if not secret:
        return False, "transfer_secret_answer не задан в конфиге"
    if not username:
        return False, "recipient username пустой"
    url = f"{base}/{item_id}/change-owner"
    params = {
        "username": username,
        "secret_answer": secret,
    }
    r = client.post(url, params=params)
    try:
        data = r.json()
    except (ValueError, TypeError):
        return False, f"HTTP {r.status_code}: {(r.text or '')[:200]}"
    if not isinstance(data, dict):
        return False, f"HTTP {r.status_code}: unexpected API response"
    if r.status_code == 200:
        return True, "ok"
    errs = data.get("errors") or data.get("message") or data
    return False, str(errs)[:300]


def get_pending_transfers(proliv_history_path: Path, transferred_path: Path) -> list[str]:
    """Возвращает draft_id из proliv_history, которые ещё не были переданы."""
    from .storage import load_transferred_items  # local import to avoid circular

    if not proliv_history_path.exists():
        return []
    already = load_transferred_items(transferred_path)
    pending: list[str] = []
    for line in proliv_history_path.read_text(encoding="utf-8", errors="replace").splitlines():
        m = re.search(r"ok:\s*draft\s+(\d+)\s+published", line)
        if m:
            draft_id = m.group(1)
            if draft_id not in already:
                pending.append(draft_id)
    return pending
