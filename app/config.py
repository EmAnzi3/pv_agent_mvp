from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    app_env: str = os.getenv("APP_ENV", "development")
    database_url: str = os.getenv(
        "DATABASE_URL",
        "sqlite:///pv_agent.db",
    )
    reports_dir: str = os.getenv("REPORTS_DIR", "reports")
    request_timeout: int = int(os.getenv("REQUEST_TIMEOUT", "30"))
    user_agent: str = os.getenv("USER_AGENT", "PV-Agent-MVP/0.1")
    enable_scheduler: bool = _as_bool(os.getenv("ENABLE_SCHEDULER"), True)
    daily_run_hour: int = int(os.getenv("DAILY_RUN_HOUR", "6"))
    daily_run_minute: int = int(os.getenv("DAILY_RUN_MINUTE", "0"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()
