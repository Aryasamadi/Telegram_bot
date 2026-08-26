"""Configuration loaded from environment variables.

Secrets never live in the repository. On Railway they are set in the service's
Variables tab; locally they come from the shell environment. Startup fails fast
with a clear list of anything missing, rather than crashing later with an
obscure error.

Secret VALUES are never logged — only whether each one is present.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger("config")


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or unusable."""


@dataclass(frozen=True)
class Settings:
    # Telegram
    telegram_bot_token: str
    bot_username: str
    admin_id: Optional[int]

    # Cloudflare D1
    cf_account_id: str
    cf_database_id: str
    cf_api_token: str

    # AI provider (optional: the bot runs without it, generation just fails)
    ai_api_key: Optional[str]
    ai_api_url: str
    ai_model_name: str

    def describe(self) -> str:
        """A safe one-line summary for startup logs: presence, never values."""
        def mark(value) -> str:
            return "set" if value else "MISSING"

        return (
            f"telegram_bot_token={mark(self.telegram_bot_token)} "
            f"cf_account_id={mark(self.cf_account_id)} "
            f"cf_database_id={mark(self.cf_database_id)} "
            f"cf_api_token={mark(self.cf_api_token)} "
            f"ai_api_key={mark(self.ai_api_key)} "
            f"ai_model_name={self.ai_model_name} "
            f"admin_id={self.admin_id if self.admin_id else 'unset'}"
        )


# These four must be present or the application refuses to start.
REQUIRED_VARS = (
    "TELEGRAM_BOT_TOKEN",
    "CF_ACCOUNT_ID",
    "CF_DATABASE_ID",
    "CF_API_TOKEN",
)

DEFAULT_AI_API_URL = "https://api.openai.com/v1/chat/completions"
DEFAULT_AI_MODEL_NAME = "gpt-4o-mini"
DEFAULT_BOT_USERNAME = "unknown_bot"


def _read_optional_int(name: str) -> Optional[int]:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        log.warning("%s is not a valid integer (%r); ignoring", name, raw)
        return None


def load_settings() -> Settings:
    """Read and validate configuration. Raises ConfigError listing everything missing."""
    missing = [name for name in REQUIRED_VARS if not os.environ.get(name, "").strip()]
    if missing:
        raise ConfigError(
            "Missing required environment variables: "
            + ", ".join(missing)
            + ". Set these in your Railway service Variables tab (or your shell) and restart."
        )

    settings = Settings(
        telegram_bot_token=os.environ["TELEGRAM_BOT_TOKEN"].strip(),
        bot_username=os.environ.get("BOT_USERNAME", "").strip() or DEFAULT_BOT_USERNAME,
        admin_id=_read_optional_int("ADMIN_ID"),
        cf_account_id=os.environ["CF_ACCOUNT_ID"].strip(),
        cf_database_id=os.environ["CF_DATABASE_ID"].strip(),
        cf_api_token=os.environ["CF_API_TOKEN"].strip(),
        ai_api_key=os.environ.get("AI_API_KEY", "").strip() or None,
        ai_api_url=os.environ.get("AI_API_URL", "").strip() or DEFAULT_AI_API_URL,
        ai_model_name=os.environ.get("AI_MODEL_NAME", "").strip() or DEFAULT_AI_MODEL_NAME,
    )

    if settings.ai_api_key is None:
        log.warning("AI_API_KEY is not set: content generation will fail until it is provided")

    return settings
