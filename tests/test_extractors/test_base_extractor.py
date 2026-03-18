"""Tests for BaseExtractor contract."""

from typing import Any

import pytest

from oren_esg.extractors.base_extractor import BaseExtractor


class ConcreteExtractor(BaseExtractor):
    """Minimal concrete subclass for testing the base behaviour."""

    def __init__(self, return_value: Any = None, raise_on_extract: bool = False):
        super().__init__(name="test_extractor")
        self._return_value = return_value
        self._raise = raise_on_extract

    def extract(self) -> Any:
        if self._raise:
            raise RuntimeError("Extraction error")
        return self._return_value


def test_run_returns_extracted_data():
    extractor = ConcreteExtractor(return_value={"score": 42})
    assert extractor.run() == {"score": 42}


def test_run_propagates_exception():
    extractor = ConcreteExtractor(raise_on_extract=True)
    with pytest.raises(RuntimeError, match="Extraction error"):
        extractor.run()
