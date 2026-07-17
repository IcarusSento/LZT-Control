"""Console logging shared by the integrated application.

Rich is used only for the interactive terminal.  Redirected output stays plain
text, so logs remain suitable for files and CI without ANSI escape sequences.
"""

from __future__ import annotations

import logging
import os
from typing import Optional


def configure_logging(level: Optional[str] = None) -> None:
    selected = (level or os.getenv("LZT_LOG_LEVEL", "INFO")).upper()
    numeric_level = getattr(logging, selected, logging.INFO)

    try:
        from rich.console import Console
        from rich.logging import RichHandler

        handler: logging.Handler = RichHandler(
            console=Console(stderr=True),
            show_time=True,
            log_time_format="%H:%M:%S",
            omit_repeated_times=False,
            show_level=True,
            show_path=False,
            rich_tracebacks=True,
            tracebacks_show_locals=False,
            markup=False,
            keywords=[
                "успешно", "Поднято", "запущен", "работает",
                "повтор", "попытка", "лимит", "Ошибка", "недоступен",
            ],
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        logging.basicConfig(level=numeric_level, handlers=[handler], force=True)
    except Exception:
        logging.basicConfig(
            level=numeric_level,
            format="%(asctime)s  %(levelname)-8s  %(message)s",
            datefmt="%H:%M:%S",
            force=True,
        )

    # Third-party request chatter hides useful application events.  Failed
    # requests are reported by our own clients with endpoint and retry count.
    for noisy in ("urllib3", "requests", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
