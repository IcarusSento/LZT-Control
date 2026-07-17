"""Validated options used by the LZT resale goods/check request."""

from __future__ import annotations

from typing import Any


EXTRA_GAME_KEYS = (
    "uplay_games",
    "ea_games",
    "ark",
    "ark_ascended",
    "warframe",
    "the_quarry",
    "brawlhalla",
)

DEFAULT_EXTRA_GAMES = {key: True for key in EXTRA_GAME_KEYS}


def normalize_extra_games(value: Any) -> dict[str, bool]:
    """Return only supported flags; only a literal true enables a flag."""
    if not isinstance(value, dict):
        return {}
    return {key: value.get(key) is True for key in EXTRA_GAME_KEYS}
