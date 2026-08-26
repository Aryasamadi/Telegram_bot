"""The USER surface: what any Telegram account can reach.

This surface is deliberately small and entirely read-only. It never touches
another person's data, so it needs no ownership guards — the only record it
reads is the caller's own, already resolved into the context before arriving
here. It also writes no audit rows: auditing every /start would spend database
writes on something with no security value.

Managers and the owner also pass through this surface. They see one extra line
pointing at their own entry command, and nothing more: no manager or owner
controls are rendered here, because these panels are separate surfaces rather
than one menu with hidden rows.
"""
from __future__ import annotations

import logging

from core.context import RequestContext, Role
from core.inbound import Inbound
from db.d1 import D1Client
from infra.telegram import TelegramClient
from ui.view import NS_USER, button, cb, esc, keyboard, reply

log = logging.getLogger("handlers.user")

_WELCOME = (
    "<b>Welcome{name}</b>\n\n"
    "This bot writes and publishes content to channels.\n\n"
    "Use the buttons below, or send /help at any time."
)

_HELP = (
    "<b>Help</b>\n\n"
    "/start — open this menu\n"
    "/status — show your account status\n"
    "/help — show this text\n\n"
    "If you were told you would have access to a channel panel and you do not "
    "see it, ask whoever invited you to finish granting access."
)


def _menu() -> dict:
    return keyboard(
        [button("My status", cb(NS_USER, "status"))],
        [button("Help", cb(NS_USER, "help"))],
    )


def _home_button() -> dict:
    return keyboard([button("Back", cb(NS_USER, "home"))])


class UserSurface:
    """Handles the user-level commands and buttons."""

    def __init__(self, db: D1Client, tg: TelegramClient) -> None:
        self._db = db
        self._tg = tg

    async def _welcome(self, ctx: RequestContext, inbound: Inbound) -> None:
        name = f", {esc(inbound.display_name)}" if inbound.display_name else ""
        await reply(self._tg, inbound, _WELCOME.format(name=name), markup=_menu())

    async def _status(self, ctx: RequestContext, inbound: Inbound) -> None:
        lines = ["<b>Your status</b>", ""]
        lines.append(f"Account: <code>{esc(inbound.telegram_id)}</code>")
        lines.append(f"Role: {esc(ctx.real_role.value)}")

        if ctx.real_role is Role.MANAGER:
            lines.append("")
            lines.append("You manage channels. Send /panel to open your panel.")
        elif ctx.real_role is Role.OWNER:
            lines.append("")
            lines.append("You are the platform owner. Send /owner to open your panel.")
        else:
            lines.append("")
            lines.append("You do not manage any channels.")

        if ctx.is_acting:
            lines.append("")
            lines.append(
                f"You are currently acting inside manager scope "
                f"<code>{esc(ctx.effective_scope)}</code>."
            )

        await reply(self._tg, inbound, "\n".join(lines), markup=_home_button())

    async def _help(self, ctx: RequestContext, inbound: Inbound) -> None:
        await reply(self._tg, inbound, _HELP, markup=_home_button())

    async def handle_command(self, ctx: RequestContext, inbound: Inbound) -> bool:
        """Route a /command. Returns False if this surface does not own it."""
        command = inbound.command

        if command == "start":
            await self._welcome(ctx, inbound)
            return True
        if command == "status":
            await self._status(ctx, inbound)
            return True
        if command == "help":
            await self._help(ctx, inbound)
            return True

        return False

    async def handle_callback(
        self,
        ctx: RequestContext,
        inbound: Inbound,
        action: str,
        parts: list,
    ) -> bool:
        """Route a button press. Returns False if the action is unknown."""
        if action in ("home", "start"):
            await self._welcome(ctx, inbound)
            return True
        if action == "status":
            await self._status(ctx, inbound)
            return True
        if action == "help":
            await self._help(ctx, inbound)
            return True

        return False
