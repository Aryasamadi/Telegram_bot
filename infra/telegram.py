"""Telegram Bot API client: long polling and outgoing messages.

Long polling means the bot asks Telegram "anything new?" and Telegram holds the
connection open until there is, or the timeout expires. No public URL or
webhook is needed, which is why this works unchanged on Railway or any host.

Rate limits (HTTP 429) are respected using Telegram's own retry_after hint.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional

import httpx

log = logging.getLogger("telegram")

_API_ROOT = "https://api.telegram.org"
_MAX_ATTEMPTS = 3


class TelegramError(RuntimeError):
    """Raised when the Telegram API reports a non-recoverable failure."""


class TelegramClient:
    def __init__(self, token: str, *, poll_timeout: int = 50) -> None:
        if not token:
            raise ValueError("TelegramClient requires a bot token")
        self._base = f"{_API_ROOT}/bot{token}"
        self._poll_timeout = poll_timeout
        # HTTP timeout must exceed the long-poll timeout or every poll aborts.
        self._client = httpx.AsyncClient(timeout=poll_timeout + 15.0)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _call(self, method: str, payload: Optional[dict] = None) -> Any:
        """POST one API method, retrying transport errors and rate limits."""
        url = f"{self._base}/{method}"
        body = payload or {}
        last_error = "no attempt made"

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                resp = await self._client.post(url, json=body)
            except httpx.HTTPError as exc:
                last_error = f"transport error: {exc}"
                log.warning("%s attempt %d failed: %s", method, attempt, exc)
                await asyncio.sleep(2 * attempt)
                continue

            if resp.status_code == 429:
                try:
                    retry_after = int(resp.json().get("parameters", {}).get("retry_after", 3))
                except (ValueError, AttributeError):
                    retry_after = 3
                log.warning("%s rate limited; waiting %ss", method, retry_after)
                await asyncio.sleep(retry_after)
                last_error = "rate limited"
                continue

            try:
                data = resp.json()
            except ValueError as exc:
                raise TelegramError(f"{method}: invalid JSON response: {exc}") from exc

            if not data.get("ok", False):
                description = data.get("description", "unknown error")
                raise TelegramError(f"{method}: {description}")

            return data.get("result")

        raise TelegramError(f"{method}: gave up after {_MAX_ATTEMPTS} attempts ({last_error})")

    async def get_me(self) -> dict:
        """Confirm the token works and return the bot's own account info."""
        return await self._call("getMe")

    async def get_updates(self, offset: Optional[int] = None) -> list:
        """Long-poll for new updates. Returns a list (possibly empty)."""
        payload: dict = {
            "timeout": self._poll_timeout,
            "allowed_updates": ["message", "callback_query"],
        }
        if offset is not None:
            payload["offset"] = offset
        result = await self._call("getUpdates", payload)
        return result if isinstance(result, list) else []

    async def send_message(
        self,
        chat_id: int,
        text: str,
        *,
        reply_markup: Optional[dict] = None,
    ) -> dict:
        """Send a text message, optionally with an inline keyboard."""
        payload: dict = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        if reply_markup is not None:
            payload["reply_markup"] = json.dumps(reply_markup)
        return await self._call("sendMessage", payload)

    async def edit_message_text(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        *,
        reply_markup: Optional[dict] = None,
    ) -> Any:
        """Replace the text of a message the bot already sent."""
        payload: dict = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
        }
        if reply_markup is not None:
            payload["reply_markup"] = json.dumps(reply_markup)
        return await self._call("editMessageText", payload)

    async def answer_callback_query(
        self,
        callback_query_id: str,
        *,
        text: Optional[str] = None,
        show_alert: bool = False,
    ) -> Any:
        """Acknowledge a button press so the loading spinner stops."""
        payload: dict = {"callback_query_id": callback_query_id, "show_alert": show_alert}
        if text is not None:
            payload["text"] = text
        return await self._call("answerCallbackQuery", payload)
