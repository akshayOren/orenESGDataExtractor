"""
Pipeline orchestrator. Wires one extractor → one transformer → one loader.
For multiple sources, instantiate multiple Pipeline objects or loop over them.
"""

from __future__ import annotations

import logging

from oren_esg.extractors.base_extractor import BaseExtractor
from oren_esg.loaders.base_loader import BaseLoader
from oren_esg.transformers.base_transformer import BaseTransformer

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(
        self,
        extractor: BaseExtractor,
        transformer: BaseTransformer,
        loader: BaseLoader,
    ) -> None:
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self) -> None:
        logger.info("Pipeline starting")
        raw = self.extractor.run()
        transformed = self.transformer.run(raw)
        self.loader.run(transformed)
        logger.info("Pipeline complete")
