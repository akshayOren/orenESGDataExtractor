"""Abstract base for all loaders."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any


class BaseLoader(ABC):
    def __init__(self, name: str) -> None:
        self.name = name
        self._logger = logging.getLogger(f"{__name__}.{name}")

    @abstractmethod
    def load(self, data: Any) -> None:
        """Write transformed data to the destination."""

    def run(self, data: Any) -> None:
        self._logger.info("Starting load: %s", self.name)
        try:
            self.load(data)
            self._logger.info("Load complete: %s", self.name)
        except Exception:
            self._logger.exception("Load failed: %s", self.name)
            raise
