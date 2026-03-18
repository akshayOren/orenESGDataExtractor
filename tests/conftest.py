"""Shared pytest fixtures available to all test modules."""

import pytest

from oren_esg.config import Settings


@pytest.fixture()
def test_settings() -> Settings:
    """Return a Settings instance with safe test defaults (no real credentials)."""
    return Settings(
        log_level="DEBUG",
        esg_api_key="test-key",
        esg_api_base_url="https://test.example.com",
    )
