from __future__ import annotations

from urllib.parse import urlsplit


_ALLOWED_LZT_API_HOSTS = {"api.lzt.market", "prod-api.lzt.market"}


def api_base(config: dict) -> str:
    value = str(config.get("lzt_api_base", "https://api.lzt.market")).strip().rstrip("/")
    parsed = urlsplit(value)
    if parsed.scheme != "https" or parsed.hostname not in _ALLOWED_LZT_API_HOSTS:
        raise ValueError(
            "Разрешены только официальные HTTPS API LZT: api.lzt.market и prod-api.lzt.market"
        )
    return value
