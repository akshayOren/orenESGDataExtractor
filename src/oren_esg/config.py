"""
Central settings object.
All configuration comes from environment variables (loaded from .env).
Import `settings` wherever you need a config value.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the project root (two levels up from this file)
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    # Pipeline behaviour
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    data_raw_dir: Path = field(
        default_factory=lambda: Path(os.getenv("DATA_RAW_DIR", "data/raw"))
    )
    data_processed_dir: Path = field(
        default_factory=lambda: Path(os.getenv("DATA_PROCESSED_DIR", "data/processed"))
    )
    log_dir: Path = field(
        default_factory=lambda: Path(os.getenv("LOG_DIR", "logs"))
    )

    # Source
    esg_api_key: str = field(default_factory=lambda: os.getenv("ESG_API_KEY", ""))
    esg_api_base_url: str = field(
        default_factory=lambda: os.getenv("ESG_API_BASE_URL", "")
    )

    # Database
    db_host: str = field(default_factory=lambda: os.getenv("DB_HOST", "localhost"))
    db_port: int = field(default_factory=lambda: int(os.getenv("DB_PORT", "5432")))
    db_name: str = field(default_factory=lambda: os.getenv("DB_NAME", "esg_data"))
    db_user: str = field(default_factory=lambda: os.getenv("DB_USER", ""))
    db_password: str = field(default_factory=lambda: os.getenv("DB_PASSWORD", ""))

    @property
    def db_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


# Single shared instance — import this everywhere
settings = Settings()
