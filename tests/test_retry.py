from __future__ import annotations

import pytest

from taskqueue.retry import backoff_seconds


def test_backoff_zero_attempt_bounded_by_base():
    for _ in range(50):
        v = backoff_seconds(0, base=1.0, cap=300.0)
        assert 0 <= v <= 1.0


def test_backoff_grows_exponentially_until_cap():
    for _ in range(50):
        v = backoff_seconds(3, base=1.0, cap=300.0)
        assert 0 <= v <= 8.0


def test_backoff_respects_cap():
    for _ in range(50):
        v = backoff_seconds(20, base=1.0, cap=300.0)
        assert 0 <= v <= 300.0


def test_negative_attempt_rejected():
    with pytest.raises(ValueError):
        backoff_seconds(-1)
