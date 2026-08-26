"""One normalized shape for everything arriving from Telegram.

Telegram sends two very different payloads: a `message` when someone types,
and a `callback_query` when someone taps an inline button. Rather than making
every handler check which arrived, both are flattened into a single `Inbound`
here, at the edge. Handlers then read one predictable object.

Note what is deliberately absent: no role, no permissions, no manager scope.
Those never come from the payload — they are loaded from stored state by
`core.resolve`. This object carries only what Telegram actually told us.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


def _display_name(user: dict) -> Optional[str]:
    """Best available human label: real name if given, else the @username."""
    first = (user.get("first_name") or "").strip()
    last = (user.get("last_name") or "").strip()
    full = " ".join(part for part in (first, last) if part)
    if full:
        return full
    username = (user.get("username") or "").strip()
    return username or None


@dataclass(frozen=True)
class Inbound:
    """A single inbound event, already flattened.

    telegram_id: the Telegram account id of the person who acted.
    chat_id: where a reply should be sent.
    display_name: a human label, for first-contact user creation only.
    text: the typed message text, or None for button presses.
    data: the callback_data of a tapped button, or None for typed messages.
    message_id: the message a button was attached to, when known.
    callback_query_id: present only for button presses; must be answered.
    """

    telegram_id: int
    chat_id: int
    display_name: Optional[str] = None
    text: Optional[str] = None
    data: Optional[str] = None
    message_id: Optional[int] = None
    callback_query_id: Optional[str] = None

    @property
    def is_callback(self) -> bool:
        """True when this came from tapping an inline button."""
        return self.callback_query_id is not None

    @property
    def command(self) -> Optional[str]:
        """The command word without its slash, or None if this isn't a command.

        Lowercased, and any `@botname` suffix removed, so that `/Start@MyBot`
        in a group behaves exactly like `/start` in a private chat.
        """
        if not self.text:
            return None
        stripped = self.text.strip()
        if not stripped.startswith("/"):
            return None
        word = stripped.split()[0][1:]
        word = word.split("@", 1)[0].strip().lower()
        return word or None

    @property
    def argument(self) -> str:
        """Everything typed after the command word, or an empty string."""
        if not self.text:
            return ""
        parts = self.text.strip().split(maxsplit=1)
        return parts[1].strip() if len(parts) > 1 else ""


def parse_update(update: dict) -> Optional[Inbound]:
    """Flatten one raw Telegram update, or return None if it isn't actionable.

    Returning None is normal and expected, not an error: Telegram delivers
    kinds of updates this bot does not handle (channel posts, edits, joins).
    The caller should quietly skip those.
    """
    if not isinstance(update, dict):
        return None

    callback = update.get("callback_query")
    if isinstance(callback, dict):
        sender = callback.get("from")
        if not isinstance(sender, dict) or sender.get("id") is None:
            return None
        message = callback.get("message")
        chat_id = None
        message_id = None
        if isinstance(message, dict):
            chat = message.get("chat")
            if isinstance(chat, dict):
                chat_id = chat.get("id")
            message_id = message.get("message_id")
        # Without a chat we can still reply privately: in a one-to-one chat the
        # chat id equals the user id.
        if chat_id is None:
            chat_id = sender["id"]
        query_id = callback.get("id")
        if query_id is None:
            return None
        return Inbound(
            telegram_id=int(sender["id"]),
            chat_id=int(chat_id),
            display_name=_display_name(sender),
            data=callback.get("data"),
            message_id=int(message_id) if message_id is not None else None,
            callback_query_id=str(query_id),
        )

    message = update.get("message")
    if isinstance(message, dict):
        sender = message.get("from")
        chat = message.get("chat")
        if not isinstance(sender, dict) or sender.get("id") is None:
            return None
        if not isinstance(chat, dict) or chat.get("id") is None:
            return None
        text = message.get("text")
        if not isinstance(text, str):
            return None  # photos, stickers, documents: nothing to route on yet
        return Inbound(
            telegram_id=int(sender["id"]),
            chat_id=int(chat["id"]),
            display_name=_display_name(sender),
            text=text,
            message_id=int(message["message_id"]) if message.get("message_id") else None,
        )

    return None
