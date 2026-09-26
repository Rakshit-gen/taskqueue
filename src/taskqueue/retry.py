"""Backoff scheduling for job retries."""

from __future__ import annotations

import random


def backoff_seconds(attempt: int, base: float = 1.0, cap: float = 300.0) -> float:
    """Exponential backoff with full jitter: random(0, min(cap, base * 2^attempt))."""
    if attempt < 0:
        raise ValueError("attempt must be >= 0")
    ceiling = min(cap, base * (2**attempt))
    return random.uniform(0, ceiling)
