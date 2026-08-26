"""Presentation layer: inline keyboards, callback encoding, and replies.

Three concerns live here because handlers need all three and none of them
belong in business logic:

1. `esc` — messages are sent with parse_mode HTML, so any text that came from
   a person or an AI model must be escaped or a stray `<` breaks the message.
2. `cb` / `parse_cb` — a tiny namespaced encoding for button data. Telegram
   caps callback_data at 64 bytes, which `cb` enforces loudly rather than
   letting Telegram reject the message later.
3. `reply` — one call that edits in place after a button press and sends a new
   message otherwise, so handlers never branch on it.
"""
from __future__ import annotations

import logging
from typing import Optional

from core.inbound import Inbound
from infra.telegram import TelegramClient, TelegramError

log = logging.getLogger("view")

# Callback namespaces. The router uses these to pick a handler surface; it is
# still the guards, never the namespace, that decide what is permitted.
NS_USER = "u"
NS_MANAGER = "m"
NS_OWNER = "o"

_CALLBACK_LIMIT = 64


def esc(text: object) -> str:
    """Escape text for HTML parse mode."""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def cb(namespace: str, action: str, *parts: object) -> str:
    """Build callback data as `namespace:action:part:part`.

    Raises ValueError if the result exceeds Telegram's 64-byte limit, so the
    problem surfaces here during development rather than as a rejected message.
    """
    encoded = ":".join([namespace, action, *(str(p) for p in parts)])
    if len(encoded.encode("utf-8")) > _CALLBACK_LIMIT:
        raise ValueError(f"callback data too long ({encoded!r})")
    return encoded


def parse_cb(data: Optional[str]) -> tuple[str, str, list[str]]:
    """Split callback data into (namespace, action, remaining parts).

    Never raises: malformed data yields empty strings, and the router treats an
    unknown route as a generic refusal. Nothing here is trusted.
    """
    if not data:
        return "", "", []
    pieces = data.split(":")
    namespace = pieces[0] if pieces else ""
    action = pieces[1] if len(pieces) > 1 else ""
    return namespace, action, pieces[2:]


def button(text: str, data: str) -> dict:
    """One inline keyboard button."""
    return {"text": text, "callback_data": data}


def keyboard(*rows: list) -> dict:
    """An inline keyboard from rows of buttons. Empty rows are dropped."""
    return {"inline_keyboard": [row for row in rows if row]}


async def reply(
    tg: TelegramClient,
    inbound: Inbound,
    text: str,
    *,
    markup: Optional[dict] = None,
) -> None:
    """Show `text` to the person who acted.

    After a button press the existing message is edited, which keeps the chat
    from filling up with near-identical menus. If editing fails for any reason
    (the message was deleted, or the text is unchanged) a fresh message is sent
    instead, so a cosmetic failure never swallows a real response.
    """
    if inbound.is_callback and inbound.message_id is not None:
        try:
            await tg.edit_message_text(
                inbound.chat_id, inbound.message_id, text, reply_markup=markup
            )
            return
        except TelegramError as exc:
            log.debug("edit failed, sending instead: %s", exc)

    await tg.send_message(inbound.chat_id, text, reply_markup=markup)
