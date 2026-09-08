from __future__ import annotations

import pytest

from event_platform.storage import Database


@pytest.fixture
def database() -> Database:
    value = Database()
    value.initialize()
    try:
        yield value
    finally:
        value.close()
