"""Open the local panel only after its HTTP server is ready."""

from __future__ import annotations

import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser


def browser_url(raw_url: str) -> str:
    """Replace wildcard bind hosts with a browser-accessible loopback host."""
    parsed = urllib.parse.urlsplit(raw_url)
    host = parsed.hostname or "127.0.0.1"
    if host in {"0.0.0.0", "::"}:
        host = "127.0.0.1"
    port = f":{parsed.port}" if parsed.port is not None else ""
    return urllib.parse.urlunsplit(
        (parsed.scheme or "http", f"{host}{port}", parsed.path or "/", "", "")
    )


def wait_and_open(raw_url: str, timeout_seconds: float = 45.0) -> bool:
    """Poll the local server and open the default browser once it responds."""
    url = browser_url(raw_url)
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            request = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(request, timeout=1.5) as response:
                if response.status < 500:
                    return bool(webbrowser.open(url, new=2))
        except (OSError, ValueError, urllib.error.URLError):
            pass
        time.sleep(0.3)
    return False


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8787"
    raise SystemExit(0 if wait_and_open(target) else 1)
