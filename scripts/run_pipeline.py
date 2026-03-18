"""
CLI entry point.
Usage:  python scripts/run_pipeline.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make `src/` importable when running from the project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oren_esg.config import settings
from oren_esg.logging_config import setup_logging

# ------- Wire up your concrete classes here --------
# from oren_esg.extractors.my_extractor import MyExtractor
# from oren_esg.transformers.my_transformer import MyTransformer
# from oren_esg.loaders.my_loader import MyLoader
# from oren_esg.pipeline import Pipeline
# ---------------------------------------------------


def main() -> None:
    setup_logging(log_level=settings.log_level, log_dir=settings.log_dir)

    # Example (uncomment and replace with real classes):
    # pipeline = Pipeline(
    #     extractor=MyExtractor(settings),
    #     transformer=MyTransformer(),
    #     loader=MyLoader(settings),
    # )
    # pipeline.run()

    print("Pipeline skeleton ready. Wire up your extractor/transformer/loader above.")


if __name__ == "__main__":
    main()
