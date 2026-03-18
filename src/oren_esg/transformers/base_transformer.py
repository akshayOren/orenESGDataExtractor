"""Abstract base for all transformers."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any


class BaseTransformer(ABC):
    def __init__(self, name: str) -> None:
        self.name = name
        self._logger = logging.getLogger(f"{__name__}.{name}")

    @abstractmethod
    def transform(self, raw_data: Any) -> Any:
        """Clean, normalize, and shape raw_data into the target schema."""

    def run(self, raw_data: Any) -> Any:
        self._logger.info("Starting transform: %s", self.name)
        try:
            result = self.transform(raw_data)
            self._logger.info("Transform complete: %s", self.name)
            return result
        except Exception:
            self._logger.exception("Transform failed: %s", self.name)
            raise
