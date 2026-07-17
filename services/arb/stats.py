import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List


def _split_history_line(line: str) -> tuple[str, str, str] | None:
    """Return (date, item_id, details) for AutoARB text histories."""
    match = re.match(
        r"^\s*(.*?)\s*\|\s*item\s+(\d+)\s*\|\s*(.*?)\s*$",
        line,
        re.IGNORECASE,
    )
    if not match:
        return None
    return match.group(1).strip(), match.group(2), match.group(3).strip()


def collect_invalid_accounts(
    validation_errors: List[Dict[str, Any]],
    pipeline_history: Path,
    claim_history: Path,
) -> List[Dict[str, Any]]:
    """Build the current invalid/KT account list, one row per account.

    Pipeline terminal events are treated as the source of truth.  Therefore a
    later successful validation removes an older invalid or KT result instead
    of leaving a permanent counter in the dashboard.
    """
    terminal: Dict[str, Dict[str, Any] | None] = {}
    if pipeline_history.exists():
        with pipeline_history.open(encoding="utf-8", errors="replace") as handle:
            for raw_line in handle:
                parsed = _split_history_line(raw_line)
                if not parsed:
                    continue
                date, item_id, details = parsed
                lower = details.casefold()
                slot_match = re.search(r"\[([^\]]+)\]\s*$", details)
                slot_label = slot_match.group(1).strip() if slot_match else "—"
                if lower.startswith("valid=fail"):
                    terminal[item_id] = {
                        "item_id": item_id,
                        "kind": "invalid",
                        "kind_label": "Невалид",
                        "reason": "Неверный логин или пароль",
                        "slot_label": slot_label,
                        "detected_at": date,
                    }
                elif lower.startswith("valid=ok kt=fail"):
                    reason = re.sub(r"^valid=ok\s+kt=fail\s*", "", details, flags=re.IGNORECASE)
                    reason = re.sub(r"\s*\[[^\]]+\]\s*$", "", reason).strip()
                    terminal[item_id] = {
                        "item_id": item_id,
                        "kind": "kt",
                        "kind_label": "КТ",
                        "reason": reason or "Ограничение Steam Community",
                        "slot_label": slot_label,
                        "detected_at": date,
                    }
                elif lower.startswith("valid=ok kt=ok"):
                    terminal[item_id] = None

    # Validation errors carry the full API explanation for invalid accounts.
    # Keep them out of the generic Errors tab, but retain the useful detail in
    # this dedicated list. A later successful pipeline event wins over stale
    # rows left by older versions.
    for error in validation_errors:
        kind = str(error.get("error_kind") or error.get("last_error_type") or "").casefold()
        if kind != "invalid_credentials":
            continue
        item_id = str(error.get("item_id") or "").strip()
        if not item_id or (item_id in terminal and terminal[item_id] is None):
            continue
        row = terminal.get(item_id) or {
            "item_id": item_id,
            "kind": "invalid",
            "kind_label": "Невалид",
            "reason": "Неверный логин или пароль",
            "slot_label": str(error.get("slot_label") or error.get("slot") or "—"),
            "detected_at": str(error.get("last_error_at") or error.get("first_error_at") or "—"),
        }
        row["reason"] = str(error.get("error_label") or error.get("error_message") or row["reason"])
        row["api_response"] = str(error.get("api_response") or "")
        terminal[item_id] = row

    claim_results: Dict[tuple[str, str], Dict[str, str]] = {}
    if claim_history.exists():
        with claim_history.open(encoding="utf-8", errors="replace") as handle:
            for raw_line in handle:
                parsed = _split_history_line(raw_line)
                if not parsed:
                    continue
                date, item_id, details = parsed
                parts = [part.strip() for part in details.split(" | ")]
                if len(parts) < 2 or parts[0].upper() not in {"REC", "KT"}:
                    continue
                claim_results[(item_id, parts[0].upper())] = {
                    "claim_status": parts[1].casefold(),
                    "claim_message": " | ".join(parts[2:]),
                    "claim_at": date,
                }

    rows: List[Dict[str, Any]] = []
    for item_id, row in terminal.items():
        if not row:
            continue
        claim_kind = "KT" if row["kind"] == "kt" else "REC"
        row.update(claim_results.get((item_id, claim_kind), {}))
        row.setdefault("claim_status", "")
        row.setdefault("claim_message", "")
        rows.append(row)

    def sort_key(row: Dict[str, Any]) -> datetime:
        raw = str(row.get("detected_at") or "")
        try:
            return datetime.strptime(raw, "%d-%m-%Y %H:%M:%S")
        except ValueError:
            return datetime.min

    rows.sort(key=sort_key, reverse=True)
    return rows


def count_file_matches(path: Path, predicate: Callable[[str], bool]) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open(encoding="utf-8", errors="replace") as f:
        for line in f:
            if line.strip() and predicate(line):
                count += 1
    return count


def count_published_items(path: Path) -> int:
    """Count unique source accounts with a confirmed publication."""
    if not path.exists():
        return 0
    item_ids: set[str] = set()
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            lower = line.casefold()
            if "ok:" not in lower or "published" not in lower:
                continue
            match = re.search(r"\bitem\s+(\d+)\b", line, re.IGNORECASE)
            if match:
                item_ids.add(match.group(1))
    return len(item_ids)


def collect_proliv_queue_errors(
    proliv: List[Dict[str, Any]], proliv_history: Path
) -> List[Dict[str, Any]]:
    """Return unresolved failures for accounts that are still in the queue.

    Only the latest history result for an account is relevant.  An old failed
    publication must not keep the error counter red after a later success.
    """
    if not proliv:
        return []
    queue = {
        str(row.get("item_id")): row
        for row in proliv
        if row.get("item_id")
    }
    latest_failures: Dict[str, str] = {}
    resolved: set[str] = set()
    if proliv_history.exists():
        with proliv_history.open(encoding="utf-8", errors="replace") as f:
            for raw_line in f:
                line = raw_line.strip()
                match = re.search(r"\bitem\s+(\d+)\b", line, re.IGNORECASE)
                if not match or match.group(1) not in queue:
                    continue
                item_id = match.group(1)
                status = line.lower()
                if re.search(r"(?:^|\|)\s*fail\b", status):
                    latest_failures[item_id] = line
                    resolved.discard(item_id)
                elif re.search(r"(?:^|\|)\s*(?:ok\b|skip\b|retry_reset\b|pending\b)", status) or "published" in status:
                    latest_failures.pop(item_id, None)
                    resolved.add(item_id)

    for item_id, row in queue.items():
        if row.get("last_error") and item_id not in latest_failures:
            latest_failures[item_id] = ""
        elif row.get("manual_review") and item_id not in latest_failures and item_id not in resolved:
            latest_failures[item_id] = ""

    errors: List[Dict[str, Any]] = []
    for item_id, line in latest_failures.items():
        row = queue[item_id]
        parts = [part.strip() for part in line.split(" | ")] if line else []
        date = parts[0] if parts else ""
        if not date and row.get("last_error_at_unix"):
            try:
                date = datetime.fromtimestamp(int(row["last_error_at_unix"])).strftime("%d-%m-%Y %H:%M:%S")
            except (TypeError, ValueError, OSError):
                date = ""
        detail = " | ".join(parts[2:]) if len(parts) > 2 else line
        detail = re.sub(r"^fail\s*:?\s*", "", detail, flags=re.IGNORECASE).strip()
        detail = str(row.get("last_error") or row.get("blocked_reason") or detail or "Ошибка публикации")
        retry_count = row.get("retry_count")
        max_retries = row.get("max_retries")
        api_response = str(row.get("api_response") or detail)
        deferred_retry = "retry_request" in f"{detail}\n{api_response}".casefold()
        if deferred_retry:
            # Internal asynchronous state: never expose it as an error, even
            # for rows saved by older versions before migration runs.
            continue
        errors.append({
            "source": "proliv",
            "err_key": f"proliv#{item_id}",
            "item_id": item_id,
            "slot": "proliv",
            "slot_label": "Пролив",
            "last_error_type": "deferred_retry" if deferred_retry else str(row.get("last_error_type") or detail),
            "error_kind": "deferred_retry" if deferred_retry else str(row.get("last_error_type") or "proliv_error"),
            "error_label": "LZT попросил повторить запрос" if deferred_retry else str(row.get("last_error_label") or "Ошибка пролива"),
            "error_message": detail,
            "stage": detail.split(":", 1)[0] if ":" in detail else "Пролив",
            "endpoint": str(row.get("endpoint") or ""),
            "http_status": row.get("http_status"),
            "api_response": api_response,
            "last_error_at": date or "—",
            "last_error_at_unix": int(row.get("last_error_at_unix", 0) or 0),
            "next_retry_at": None if row.get("manual_review") else str(row.get("run_at_str") or "По расписанию"),
            "retry_count": retry_count,
            "max_retries": max_retries,
            "exhausted": bool(row.get("manual_review")),
        })
    return errors


def filter_active_guarantee_records(
    records: List[Dict[str, Any]], *, now: datetime | None = None
) -> List[Dict[str, Any]]:
    current = now or datetime.now()
    active: List[Dict[str, Any]] = []
    for record in records:
        value = str(record.get("value") or "")
        lower = value.casefold()
        if "гарантия истекла" in lower or re.search(r"\bexpired\b", lower):
            continue
        match = re.search(r"(\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}:\d{2})", value)
        if match:
            try:
                if datetime.strptime(match.group(1), "%d-%m-%Y %H:%M:%S") <= current:
                    continue
            except ValueError:
                pass
        active.append(record)
    return active


def collect_tracked_guarantee_records(
    records: List[Dict[str, Any]], tracked_item_ids: set[str]
) -> List[Dict[str, Any]]:
    """Return one guarantee row for every account still in the workflow.

    The dashboard account counter is built from scheduled checks and the
    publication queue.  The guarantees page must use the same source of truth:
    a warranty date may already be in the past while the account is waiting for
    publication.  Such an account remains visible until publication finishes.
    """
    wanted = {str(item_id) for item_id in tracked_item_ids if str(item_id)}
    latest: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for record in records:
        item_id = str(record.get("item_id") or "")
        if item_id not in wanted:
            continue
        if item_id not in latest:
            order.append(item_id)
        latest[item_id] = record

    for item_id in sorted(wanted - set(latest)):
        latest[item_id] = {
            "line_index": -1,
            "value": f"Item ID: {item_id}, Статус гарантии: ожидает обработки",
            "item_id": item_id,
            "synthetic": True,
        }
        order.append(item_id)
    return [latest[item_id] for item_id in order]


def compute_dashboard_stats(
    checks: List[Dict[str, Any]],
    proliv: List[Dict[str, Any]],
    val_errors: List[Dict[str, Any]],
    *,
    valid_history: Path,
    claim_history: Path,
    proliv_history: Path,
    pipeline_history: Path | None = None,
) -> Dict[str, int]:
    account_ids = {str(c.get("item_id")) for c in checks if c.get("item_id")}
    account_ids |= {str(p.get("item_id")) for p in proliv if p.get("item_id")}
    proliv_errors = len(collect_proliv_queue_errors(proliv, proliv_history))
    forsale = count_published_items(proliv_history)
    arbitr = count_file_matches(claim_history, lambda _line: True)
    history = (
        count_file_matches(valid_history, lambda _line: True)
        + arbitr
        + count_file_matches(proliv_history, lambda _line: True)
        + (count_file_matches(pipeline_history, lambda _line: True) if pipeline_history else 0)
    )
    return {
        "accounts": len(account_ids),
        "checks": len(checks),
        "proliv": len(proliv),
        "errors": len(val_errors) + proliv_errors,
        "invalid": count_file_matches(valid_history, lambda ln: " invalid" in ln.lower()),
        "kt": count_file_matches(claim_history, lambda ln: "| kt |" in ln.lower()),
        "val_errors": len(val_errors),
        "val_exhausted": sum(1 for e in val_errors if e.get("exhausted")),
        "proliv_errors": proliv_errors,
        "history": history,
        "forsale": forsale,
        "arbitr": arbitr,
    }
