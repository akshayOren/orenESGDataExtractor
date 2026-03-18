"""Abstract base for all extractors. Each data source gets its own subclass."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """
    Subclass this for every ESG data source.

    Minimal contract:
        extractor = MyExtractor(config)
        raw_data = extractor.extract()
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._logger = logging.getLogger(f"{__name__}.{name}")

    @abstractmethod
    def extract(self) -> Any:
        """Pull raw data from the source. Return type depends on the source."""

    def run(self) -> Any:
        """Wraps extract() with logging and basic error handling."""
        self._logger.info("Starting extraction: %s", self.name)
        try:
            result = self.extract()
            self._logger.info("Extraction complete: %s", self.name)
            return result
        except Exception:
            self._logger.exception("Extraction failed: %s", self.name)
            raise
