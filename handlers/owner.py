"""The PLATFORM OWNER surface: managers, platform overview, and acting mode.

This is a separate surface from the manager panel, not a superset of it. The
owner does not get extra buttons inside someone else's panel; instead the owner
explicitly *enters* a manager's scope, and from then on sees exactly what that
manager sees, with a visible banner saying so.

Entering acting mode changes what the owner can reach. It does not change who
the owner is. `real_user_id` and `real_role` stay fixed for the entire session,
so every audit row written while acting names the owner's own account — never
the borrowed manager. That is the property that makes acting mode auditable
rather than a way to act anonymously.

Every method re-checks that the caller really is the owner. The router already
checks before routing here, so this is redundant by design: a routing mistake
should fail closed rather than expose the platform panel.
"""
from __future__ import annotations

import logging
from typing import Optional

from core.acting import ActingStore
from core.context import RequestContext, Role
from core.guards import OwnershipError, audit, require_manager_owned
from core.inbound import Inbound
from db.d1 import D1Client
from infra.telegram import TelegramClient
from ui.view import NS_MANAGER, NS_OWNER, NS_USER, button, cb, esc, keyboard, reply

log = logging.getLogger("handlers.owner")

_LIST_LIMIT = 10


def _require_owner(ctx: RequestContext) -> None:
    """Fail closed unless the stored record says this caller is the owner."""
    if ctx.real_role is not Role.OWNER:
        raise OwnershipError(
            f"role {ctx.real_role.value} may not use the owner surface"
        )


def _menu(acting_scope: Optional[int]) -> dict:
    rows = [
        [button("Managers", cb(NS_OWNER, "managers"))],
        [button("Platform overview", cb(NS_OWNER, "overview"))],
    ]
    if acting_scope is not None:
        rows.append([button("Open manager panel", cb(NS_MANAGER, "panel"))])
        rows.append([button("Stop acting", cb(NS_OWNER, "stop"))])
    rows.append([button("Exit to main menu", cb(NS_USER, "home"))])
    return keyboard(*rows)


def _back_to_panel() -> dict:
    return keyboard([button("Back", cb(NS_OWNER, "panel"))])


async def _count(db: D1Client, sql: str, params: Optional[list] = None) -> int:
    """Run a COUNT query and return a plain integer."""
    row = await db.fetchone(sql, params or [])
    if not row:
        return 0
    value = row.get("n")
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


class OwnerSurface:
    """Handles the owner-level commands and buttons."""

    def __init__(self, db: D1Client, tg: TelegramClient, acting: ActingStore) -> None:
        self._db = db
        self._tg = tg
        self._acting = acting

    async def _panel(self, ctx: RequestContext, inbound: Inbound) -> None:
        _require_owner(ctx)
        scope = self._acting.current(inbound.telegram_id)

        lines = ["<b>Owner panel</b>", ""]
        if scope is not None:
            lines.append(
                f"You are acting inside manager scope <code>{esc(scope)}</code>. "
                "Everything you do is recorded against your own account."
            )
        else:
            lines.append("You are acting as yourself.")

        await reply(self._tg, inbound, "\n".join(lines), markup=_menu(scope))

    async def _overview(self, ctx: RequestContext, inbound: Inbound) -> None:
        _require_owner(ctx)

        users = await _count(self._db, "SELECT COUNT(*) AS n FROM users")
        managers = await _count(self._db, "SELECT COUNT(*) AS n FROM managers")
        enabled_managers = await _count(
            self._db, "SELECT COUNT(*) AS n FROM managers WHERE enabled = 1"
        )
        channels = await _count(self._db, "SELECT COUNT(*) AS n FROM channels")
        drafts = await _count(
            self._db, "SELECT COUNT(*) AS n FROM queue WHERE status = 'draft'"
        )
        published = await _count(
            self._db, "SELECT COUNT(*) AS n FROM queue WHERE status = 'published'"
        )
        failed = await _count(
            self._db, "SELECT COUNT(*) AS n FROM queue WHERE status = 'failed'"
        )

        text = (
            "<b>Platform overview</b>\n\n"
            f"Accounts known: {users}\n"
            f"Managers: {managers} ({enabled_managers} enabled)\n"
            f"Channels: {channels}\n\n"
            f"Queue — drafts: {drafts}\n"
            f"Queue — published: {published}\n"
            f"Queue — failed: {failed}"
        )
        await reply(self._tg, inbound, text, markup=_back_to_panel())

    async def _managers(self, ctx: RequestContext, inbound: Inbound) -> None:
        _require_owner(ctx)

        rows = await self._db.query(
            "SELECT id, name, enabled FROM managers ORDER BY id LIMIT ?",
            [_LIST_LIMIT],
        )
        if not rows:
            await reply(
                self._tg,
                inbound,
                "<b>Managers</b>\n\nNo managers exist yet.",
                markup=_back_to_panel(),
            )
            return

        buttons = []
        for row in rows:
            mark = "on" if row.get("enabled") else "off"
            label = f"{row.get('name') or 'unnamed'} ({mark})"
            buttons.append([button(label, cb(NS_OWNER, "manager", row["id"]))])
        buttons.append([button("Back", cb(NS_OWNER, "panel"))])

        await reply(
            self._tg,
            inbound,
            "<b>Managers</b>\n\nSelect a manager.",
            markup=keyboard(*buttons),
        )
          async def _manager(
        self, ctx: RequestContext, inbound: Inbound, manager_id: int
    ) -> None:
        _require_owner(ctx)
        row = await require_manager_owned(self._db, ctx, manager_id)

        channels = await _count(
            self._db,
            "SELECT COUNT(*) AS n FROM channels WHERE manager_id = ?",
            [manager_id],
        )
        pending = await _count(
            self._db,
            "SELECT COUNT(*) AS n FROM queue WHERE manager_id = ? AND status = 'draft'",
            [manager_id],
        )
        enabled = bool(row.get("enabled"))

        text = (
            f"<b>{esc(row.get('name') or 'unnamed')}</b>\n\n"
            f"Scope id: <code>{esc(manager_id)}</code>\n"
            f"Status: {'enabled' if enabled else 'disabled'}\n"
            f"Channels: {channels}\n"
            f"Drafts waiting: {pending}"
        )

        toggle_label = "Disable manager" if enabled else "Enable manager"
        await reply(
            self._tg,
            inbound,
            text,
            markup=keyboard(
                [button("Act as this manager", cb(NS_OWNER, "act", manager_id))],
                [button(toggle_label, cb(NS_OWNER, "mtoggle", manager_id))],
                [button("Back", cb(NS_OWNER, "managers"))],
            ),
        )

    async def _toggle_manager(
        self, ctx: RequestContext, inbound: Inbound, manager_id: int
    ) -> None:
        _require_owner(ctx)
        row = await require_manager_owned(self._db, ctx, manager_id)
        new_value = 0 if row.get("enabled") else 1

        await self._db.execute(
            "UPDATE managers SET enabled = ? WHERE id = ?", [new_value, manager_id]
        )
        await audit(
            self._db,
            ctx,
            "manager.toggle",
            target=str(manager_id),
            detail=f"enabled={new_value}",
        )
        await self._manager(ctx, inbound, manager_id)

    async def _act_as(
        self, ctx: RequestContext, inbound: Inbound, manager_id: int
    ) -> None:
        _require_owner(ctx)
        # Verify the scope exists before granting it. Acting into a manager id
        # that does not exist would produce a context whose queries silently
        # return nothing, which is confusing rather than safe.
        row = await require_manager_owned(self._db, ctx, manager_id)

        self._acting.begin(inbound.telegram_id, manager_id)
        # This row is written before the acting session takes effect for the
        # request, so acting_context still shows the owner's own scope. That is
        # accurate: it records the moment the request was made, and `target`
        # names the scope being entered.
        await audit(
            self._db, ctx, "acting.begin", target=str(manager_id),
            detail=f"name={row.get('name')}",
        )

        text = (
            f"You are now acting inside <b>{esc(row.get('name') or manager_id)}</b>.\n\n"
            "You will see exactly what that manager sees. Every action you take "
            "is recorded against your own account, not theirs.\n\n"
            "This ends automatically after a couple of hours, or when you stop it."
        )
        await reply(
            self._tg,
            inbound,
            text,
            markup=keyboard(
                [button("Open manager panel", cb(NS_MANAGER, "panel"))],
                [button("Stop acting", cb(NS_OWNER, "stop"))],
            ),
        )

    async def _stop_acting(self, ctx: RequestContext, inbound: Inbound) -> None:
        _require_owner(ctx)
        previous = self._acting.current(inbound.telegram_id)
        self._acting.end(inbound.telegram_id)

        if previous is not None:
            await audit(self._db, ctx, "acting.end", target=str(previous))

        await reply(
            self._tg,
            inbound,
            "You are acting as yourself again.",
            markup=_menu(None),
        )

    async def handle_command(self, ctx: RequestContext, inbound: Inbound) -> bool:
        """Route a /command. Returns False if this surface does not own it."""
        command = inbound.command
        if command == "owner":
            await self._panel(ctx, inbound)
            return True
        if command == "stopacting":
            await self._stop_acting(ctx, inbound)
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
        if action == "panel":
            await self._panel(ctx, inbound)
            return True
        if action == "overview":
            await self._overview(ctx, inbound)
            return True
        if action == "managers":
            await self._managers(ctx, inbound)
            return True
        if action == "stop":
            await self._stop_acting(ctx, inbound)
            return True

        if not parts:
            return False
        try:
            manager_id = int(parts[0])
        except (TypeError, ValueError):
            return False

        if action == "manager":
            await self._manager(ctx, inbound, manager_id)
            return True
        if action == "mtoggle":
            await self._toggle_manager(ctx, inbound, manager_id)
            return True
        if action == "act":
            await self._act_as(ctx, inbound, manager_id)
            return True

        return False
