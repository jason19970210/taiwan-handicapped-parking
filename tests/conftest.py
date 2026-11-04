"""Shared pytest fixtures. Tests never touch the network."""

import pytest


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    """Retry backoff must never actually sleep in tests."""
    import time

    monkeypatch.setattr(time, "sleep", lambda *_a, **_k: None)
