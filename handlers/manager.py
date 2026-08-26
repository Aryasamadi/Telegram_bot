"""The MANAGER surface: channels and the publication queue for one tenant.

Every read and write in this file is scoped to a single manager. That scoping
is never taken from the button that was pressed — it comes from
`ctx.effective_scope`, which was established from stored state before the
request reached here. A button carrying `m:channel:999` therefore cannot reach
channel 999 unless that channel genuinely belongs to the caller's scope:
`require_channel_owned` re-checks ownership in the database on every single
call, and raises rather than returning something harmless-looking.

The same rule governs publishing. The target channel is read from the queue
row the database returned, never from the callback payload, so a forged button
cannot redirect someone else's post into a channel of the attacker's choosing.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from core.context import RequestContext
from core.guards import (
    OwnershipError,
    audit,
    require_channel_owned,
    require_queue_item_owned,
)
from core.inbound import Inbound
from db.d1 import D1Client
from infra.telegram import TelegramClient, TelegramError
from ui.view import NS_MANAGER, NS_USER, button, cb, esc, keyboard, reply

log = logging.getLogger("handlers.manager")

# How many rows a list shows at once. Telegram renders long keyboards badly,
# and this keeps every message comfortably inside the API's size limits.
_LIST_LIMIT = 8

# Length of the body excerpt shown when reviewing a queue item.
_PREVIEW_CHARS = 600

_STATUS_LABELS = {
    "draft": "draft",
    "publishing": "publishing…",
    "published": "published",
    "failed": "failed",
    "discarded": "discarded",
}

# Statuses a post may be published from. 'publishing' is absent on purpose:
# that is the in-flight marker, and re-entering it is what double-tapping a
# button would otherwise do.
_PUBLISHABLE = ("draft", "failed")


def _status_label(raw: object) -> str:
    return _STATUS_LABELS.get(str(raw), str(raw))


def _menu() -> dict:
    return keyboard(
        [button("Channels", cb(NS_MANAGER, "channels"))],
        [button("Queue", cb(NS_MANAGER, "queue"))],
        [button("Exit to main menu", cb(NS_USER, "home"))],
    )


def _back_to_panel() -> dict:
    return keyboard([button("Back", cb(NS_MANAGER, "panel"))])


def _acting_banner(ctx: RequestContext) -> str:
    """A visible warning when the caller is borrowing someone else's scope."""
    if not ctx.is_acting:
        return ""
    return (
        f"\n<i>Acting inside manager scope "
        f"<code>{esc(ctx.effective_scope)}</code>. Actions are recorded "
        f"against your own account.</i>\n"
    )


class ManagerSurface:
    """Handles the manager-level commands and buttons."""

    def __init__(self, db: D1Client, tg: TelegramClient) -> None:
        self._db = db
        self._tg = tg

    async def _panel(self, ctx: RequestContext, inbound: Inbound) -> None:
        if ctx.effective_scope is None:
            await reply(
                self._tg,
                inbound,
                "<b>Manager panel</b>\n\nYour account has no channel scope "
                "assigned yet, so there is nothing to manage. Ask the platform "
                "owner to finish setting up your access.",
            )
            return

        text = (
            "<b>Manager panel</b>\n"
            f"{_acting_banner(ctx)}\n"
            "Choose what you want to work on."
        )
        await reply(self._tg, inbound, text, markup=_menu())

    async def _channels(self, ctx: RequestContext, inbound: Inbound) -> None:
        scope = ctx.effective_scope
        if scope is None:
            await self._panel(ctx, inbound)
            return

        rows = await self._db.query(
            "SELECT id, title, enabled FROM channels "
            "WHERE manager_id = ? ORDER BY id LIMIT ?",
            [scope, _LIST_LIMIT],
        )

        if not rows:
            await reply(
                self._tg,
                inbound,
                "<b>Channels</b>\n\nNo channels are registered in your scope yet.",
                markup=_back_to_panel(),
            )
            return

        buttons = []
        for row in rows:
            mark = "on" if row.get("enabled") else "off"
            label = f"{row.get('title') or 'untitled'} ({mark})"
            buttons.append([button(label, cb(NS_MANAGER, "channel", row["id"]))])
        buttons.append([button("Back", cb(NS_MANAGER, "panel"))])

        await reply(
            self._tg,
            inbound,
            f"<b>Channels</b>{_acting_banner(ctx)}\n\nSelect a channel.",
            markup=keyboard(*buttons),
        )

    async def _channel(
        self, ctx: RequestContext, inbound: Inbound, channel_id: int
    ) -> None:
        row = await require_channel_owned(self._db, ctx, channel_id)
        enabled = bool(row.get("enabled"))

        text = (
            f"<b>{esc(row.get('title') or 'untitled')}</b>\n"
            f"{_acting_banner(ctx)}\n"
            f"Telegram id: <code>{esc(row.get('telegram_channel_id'))}</code>\n"
            f"Publishing: {'enabled' if enabled else 'disabled'}"
        )
        toggle_label = "Disable publishing" if enabled else "Enable publishing"

        await reply(
            self._tg,
            inbound,
            text,
            markup=keyboard(
                [button(toggle_label, cb(NS_MANAGER, "chtoggle", channel_id))],
                [button("Back", cb(NS_MANAGER, "channels"))],
            ),
        )

    async def _toggle_channel(
        self, ctx: RequestContext, inbound: Inbound, channel_id: int
    ) -> None:
        row = await require_channel_owned(self._db, ctx, channel_id)
        new_value = 0 if row.get("enabled") else 1

        # The scope is repeated in the WHERE clause even though ownership was
        # just verified. It costs nothing and means a mistake in the guard
        # cannot turn into a cross-tenant write.
        await self._db.execute(
            "UPDATE channels SET enabled = ? WHERE id = ? AND manager_id = ?",
            [new_value, channel_id, ctx.effective_scope],
        )
        await audit(
            self._db,
            ctx,
            "channel.toggle",
            target=str(channel_id),
            detail=f"enabled={new_value}",
        )
        await self._channel(ctx, inbound, channel_id)
          async def _queue(self, ctx: RequestContext, inbound: Inbound) -> None:
        scope = ctx.effective_scope
        if scope is None:
            await self._panel(ctx, inbound)
            return

        rows = await self._db.query(
            "SELECT id, title, status FROM queue "
            "WHERE manager_id = ? ORDER BY id DESC LIMIT ?",
            [scope, _LIST_LIMIT],
        )

        if not rows:
            await reply(
                self._tg,
                inbound,
                "<b>Queue</b>\n\nNothing is waiting in your queue.",
                markup=_back_to_panel(),
            )
            return

        buttons = []
        for row in rows:
            title = row.get("title") or "untitled"
            if len(title) > 30:
                title = title[:29] + "…"
            label = f"{title} — {_status_label(row.get('status'))}"
            buttons.append([button(label, cb(NS_MANAGER, "item", row["id"]))])
        buttons.append([button("Back", cb(NS_MANAGER, "panel"))])

        await reply(
            self._tg,
            inbound,
            f"<b>Queue</b>{_acting_banner(ctx)}\n\nMost recent first.",
            markup=keyboard(*buttons),
        )

    async def _queue_item(
        self, ctx: RequestContext, inbound: Inbound, item_id: int
    ) -> None:
        row = await require_queue_item_owned(self._db, ctx, item_id)
        status = str(row.get("status") or "draft")

        body = str(row.get("body") or "")
        excerpt = body[:_PREVIEW_CHARS]
        if len(body) > _PREVIEW_CHARS:
            excerpt += "…"

        lines = [
            f"<b>{esc(row.get('title') or 'untitled')}</b>",
            _acting_banner(ctx),
            f"Status: {esc(_status_label(status))}",
        ]
        if row.get("error"):
            lines.append(f"Last error: {esc(row['error'])}")
        lines.append("")
        lines.append(esc(excerpt) if excerpt else "<i>(no body text)</i>")

        buttons = []
        if status in _PUBLISHABLE:
            buttons.append([button("Publish now", cb(NS_MANAGER, "publish", item_id))])
            buttons.append([button("Discard", cb(NS_MANAGER, "discard", item_id))])
        buttons.append([button("Back", cb(NS_MANAGER, "queue"))])

        await reply(self._tg, inbound, "\n".join(lines), markup=keyboard(*buttons))

    async def _publish(
        self, ctx: RequestContext, inbound: Inbound, item_id: int
    ) -> None:
        row = await require_queue_item_owned(self._db, ctx, item_id)
        scope = ctx.effective_scope

        # Claim the item before sending anything. If two taps arrive at once,
        # only one UPDATE reports a change, and only that one proceeds. Without
        # this, a double-tap would post the same content to the channel twice.
        claimed = await self._db.execute(
            "UPDATE queue SET status = 'publishing' "
            "WHERE id = ? AND manager_id = ? AND status IN ('draft', 'failed')",
            [item_id, scope],
        )
        if claimed != 1:
            await reply(
                self._tg,
                inbound,
                "That item is no longer publishable — it may already have been "
                "published or discarded.",
                markup=keyboard([button("Back", cb(NS_MANAGER, "queue"))]),
            )
            return

        # The destination comes from the stored row, never from the button.
        channel = await require_channel_owned(self._db, ctx, int(row["channel_id"]))
        if not channel.get("enabled"):
            await self._db.execute(
                "UPDATE queue SET status = 'draft', error = ? "
                "WHERE id = ? AND manager_id = ?",
                ["publishing is disabled for this channel", item_id, scope],
            )
            await reply(
                self._tg,
                inbound,
                "Publishing is disabled for that channel. Enable it first, then "
                "try again.",
                markup=keyboard([button("Back", cb(NS_MANAGER, "item", item_id))]),
            )
            return

        title = str(row.get("title") or "").strip()
        body = str(row.get("body") or "").strip()
        message = f"<b>{esc(title)}</b>\n\n{esc(body)}" if title else esc(body)

        try:
            await self._tg.send_message(int(channel["telegram_channel_id"]), message)
        except TelegramError as exc:
            log.warning("publish failed for item %s: %s", item_id, exc)
            await self._db.execute(
                "UPDATE queue SET status = 'failed', error = ? "
                "WHERE id = ? AND manager_id = ?",
                [str(exc)[:400], item_id, scope],
            )
            await audit(
                self._db, ctx, "queue.publish.failed",
                target=str(item_id), detail=str(exc)[:200],
            )
            await reply(
                self._tg,
                inbound,
                f"Publishing failed: {esc(str(exc)[:300])}\n\nThe item was kept "
                "so you can retry.",
                markup=keyboard([button("Back", cb(NS_MANAGER, "item", item_id))]),
            )
            return

        await self._db.execute(
            "UPDATE queue SET status = 'published', published_at = ?, error = NULL "
            "WHERE id = ? AND manager_id = ?",
            [int(time.time()), item_id, scope],
        )
        await audit(
            self._db, ctx, "queue.publish",
            target=str(item_id), detail=f"channel={channel['id']}",
        )
        await reply(
            self._tg,
            inbound,
            "Published.",
            markup=keyboard([button("Back to queue", cb(NS_MANAGER, "queue"))]),
        )

    async def _discard(
        self, ctx: RequestContext, inbound: Inbound, item_id: int
    ) -> None:
        await require_queue_item_owned(self._db, ctx, item_id)
        await self._db.execute(
            "UPDATE queue SET status = 'discarded' "
            "WHERE id = ? AND manager_id = ? AND status IN ('draft', 'failed')",
            [item_id, ctx.effective_scope],
        )
        await audit(self._db, ctx, "queue.discard", target=str(item_id))
        await reply(
            self._tg,
            inbound,
            "Discarded.",
            markup=keyboard([button("Back to queue", cb(NS_MANAGER, "queue"))]),
        )

    async def handle_command(self, ctx: RequestContext, inbound: Inbound) -> bool:
        """Route a /command. Returns False if this surface does not own it."""
        if inbound.command == "panel":
            await self._panel(ctx, inbound)
            return True
        return False

    async def handle_callback(
        self,
        ctx: RequestContext,
        inbound: Inbound,
        action: str,
        parts: list,
    ) -> bool:
        """Route a button press. Returns False if the action is unknown.

        Ids arriving in `parts` are attacker-controlled strings. They are only
        converted to integers here; whether they may be touched is decided by
        the require_* guards, against the database, every time.
        """
        def first_id() -> Optional[int]:
            if not parts:
                return None
            try:
                return int(parts[0])
            except (TypeError, ValueError):
                return None

        if action == "panel":
            await self._panel(ctx, inbound)
            return True
        if action == "channels":
            await self._channels(ctx, inbound)
            return True
        if action == "queue":
            await self._queue(ctx, inbound)
            return True

        item_id = first_id()
        if item_id is None:
            return False

        if action == "channel":
            await self._channel(ctx, inbound, item_id)
            return True
        if action == "chtoggle":
            await self._toggle_channel(ctx, inbound, item_id)
            return True
        if action == "item":
            await self._queue_item(ctx, inbound, item_id)
            return True
        if action == "publish":
            await self._publish(ctx, inbound, item_id)
            return True
        if action == "discard":
            await self._discard(ctx, inbound, item_id)
            return True

        return False
