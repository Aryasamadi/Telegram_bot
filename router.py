"""The single entry point for every inbound Telegram event.

Three responsibilities, in this order:

1. Establish identity from stored state. Nothing about role or scope is read
   from the payload; `resolve_context` loads it from the database.
2. Gate by role, then hand off to exactly one surface. The user, manager, and
   owner surfaces are separate objects, and a request reaches at most one.
3. Fail closed and fail quiet. A refused request and a nonexistent route
   produce the identical response, so probing reveals nothing about what
   exists. Internal errors are logged in full and reported to the person as a
   single bland sentence — no stack traces, no SQL, no ids.

`handle_update` never raises. The polling loop calls it for every update, and
one malformed message must not be able to stop the bot.
"""
from __future__ import annotations

import logging

from config import Settings
from core.acting import ActingStore
from core.context import RequestContext, Role
from core.guards import OwnershipError, audit
from core.inbound import Inbound, parse_update
from core.resolve import resolve_context
from db.d1 import D1Client
from handlers.manager import ManagerSurface
from handlers.owner import OwnerSurface
from handlers.user import UserSurface
from infra.telegram import TelegramClient, TelegramError
from ui.view import NS_MANAGER, NS_OWNER, NS_USER, parse_cb

log = logging.getLogger("router")

# One message for every refusal, whatever the reason. Deliberately uninformative.
_REFUSED = "That isn't available."

# One message for every internal failure. Details go to the logs only.
_FAILED = "Something went wrong. Please try again in a moment."

_UNKNOWN_TEXT = (
    "I didn't understand that. Send /start to open the menu, or /help for the "
    "list of commands."
)

# Surfaces a manager-namespace request may come from. The owner is included
# because acting mode means entering the manager panel deliberately.
_MANAGER_ROLES = (Role.MANAGER, Role.OWNER)


class Router:
    """Receives raw Telegram updates and dispatches them to one surface."""

    def __init__(
        self,
        db: D1Client,
        tg: TelegramClient,
        acting: ActingStore,
        settings: Settings,
    ) -> None:
        self._db = db
        self._tg = tg
        self._acting = acting
        self._settings = settings
        self._user = UserSurface(db, tg)
        self._manager = ManagerSurface(db, tg)
        self._owner = OwnerSurface(db, tg, acting)

    async def handle_update(self, update: dict) -> None:
        """Handle one raw update. Never raises."""
        inbound = parse_update(update)
        if inbound is None:
            return  # not an update kind this bot acts on

        # Acknowledge button presses immediately so the spinner stops, whatever
        # happens next. A failure here is cosmetic and must not abort the work.
        if inbound.is_callback and inbound.callback_query_id is not None:
            try:
                await self._tg.answer_callback_query(inbound.callback_query_id)
            except TelegramError as exc:
                log.debug("callback ack failed: %s", exc)

        try:
            await self._dispatch(inbound)
        except (OwnershipError, PermissionError) as exc:
            # Expected outcome, not a bug: someone reached for something outside
            # their scope. Recorded with detail in the log, refused blandly.
            log.warning(
                "refused account=%s data=%r text=%r: %s",
                inbound.telegram_id, inbound.data, inbound.command, exc,
            )
            await self._say(inbound, _REFUSED)
        except Exception:
            log.exception("unhandled error for account=%s", inbound.telegram_id)
            await self._say(inbound, _FAILED)

    async def _dispatch(self, inbound: Inbound) -> None:
        """Resolve identity, then route. May raise; handle_update catches."""
        claim = self._acting.current(inbound.telegram_id)

        try:
            ctx = await resolve_context(
                self._db,
                inbound.telegram_id,
                display_name=inbound.display_name,
                acting_manager_id=claim,
            )
        except PermissionError:
            # The stored acting claim is no longer allowed — for instance the
            # account was demoted while a session was open. Drop the claim and
            # continue as themselves rather than locking them out entirely.
            log.warning(
                "dropping stale acting claim for account=%s scope=%s",
                inbound.telegram_id, claim,
            )
            self._acting.end(inbound.telegram_id)
            ctx = await resolve_context(
                self._db,
                inbound.telegram_id,
                display_name=inbound.display_name,
            )

        ctx = await self._bootstrap_owner(ctx, inbound)

        if inbound.is_callback:
            await self._route_callback(ctx, inbound)
        else:
            await self._route_command(ctx, inbound)

    async def _bootstrap_owner(
        self, ctx: RequestContext, inbound: Inbound
    ) -> RequestContext:
        """Promote the configured ADMIN_ID to owner on first contact.

        Without this there is no way to create the first owner, since every new
        account is stored as an ordinary user — and promoting one by hand would
        mean running SQL against the database, which this project does not do.

        The promotion is driven entirely by the ADMIN_ID environment variable,
        which only the deployer can set. It is idempotent: once the stored role
        is already owner, nothing is written.
        """
        admin_id = self._settings.admin_id
        if admin_id is None or inbound.telegram_id != admin_id:
            return ctx
        if ctx.real_role is Role.OWNER:
            return ctx

        await self._db.execute(
            "UPDATE users SET real_role = ? WHERE id = ?",
            [Role.OWNER.value, ctx.real_user_id],
        )
        log.info("promoted account %s to owner via ADMIN_ID", inbound.telegram_id)
        await audit(
            self._db, ctx, "owner.bootstrap", target=str(inbound.telegram_id)
        )

        # Re-resolve so the rest of this request runs with the new role rather
        # than the stale one this request started with.
        return await resolve_context(
            self._db,
            inbound.telegram_id,
            display_name=inbound.display_name,
            acting_manager_id=self._acting.current(inbound.telegram_id),
        )

    async def _route_callback(self, ctx: RequestContext, inbound: Inbound) -> None:
        """Send a button press to the one surface that owns its namespace.

        The namespace comes from attacker-controlled data, so it selects a
        surface but grants nothing: the role gate here, and the ownership guards
        inside each surface, decide what actually happens. A failed gate leaves
        `handled` False and produces the same refusal as an unknown route.
        """
        namespace, action, parts = parse_cb(inbound.data)
        handled = False

        if namespace == NS_OWNER:
            if ctx.real_role is Role.OWNER:
                handled = await self._owner.handle_callback(ctx, inbound, action, parts)
        elif namespace == NS_MANAGER:
            if ctx.real_role in _MANAGER_ROLES:
                handled = await self._manager.handle_callback(ctx, inbound, action, parts)
        elif namespace == NS_USER:
            handled = await self._user.handle_callback(ctx, inbound, action, parts)

        if not handled:
            log.info(
                "unrouted callback account=%s data=%r role=%s",
                inbound.telegram_id, inbound.data, ctx.real_role.value,
            )
            await self._say(inbound, _REFUSED)

    async def _route_command(self, ctx: RequestContext, inbound: Inbound) -> None:
        """Send a typed message to the most specific surface that claims it.

        Order matters: owner commands are offered first, then manager, then
        user. Each surface returns False for anything it does not own, so a
        command falls through to the general surface rather than being blocked
        by a more privileged one.
        """
        if inbound.command is None:
            await self._say(inbound, _UNKNOWN_TEXT)
            return

        if ctx.real_role is Role.OWNER:
            if await self._owner.handle_command(ctx, inbound):
                return

        if ctx.real_role in _MANAGER_ROLES:
            if await self._manager.handle_command(ctx, inbound):
                return

        if await self._user.handle_command(ctx, inbound):
            return

        log.info(
            "unknown command account=%s command=%r role=%s",
            inbound.telegram_id, inbound.command, ctx.real_role.value,
        )
        await self._say(inbound, _REFUSED)

    async def _say(self, inbound: Inbound, text: str) -> None:
        """Send a plain message, swallowing delivery failures.

        Used for refusals and error notices. If Telegram will not accept the
        message there is nothing further to try, and raising here would turn a
        handled refusal into an unhandled crash.
        """
        try:
            await self._tg.send_message(inbound.chat_id, text)
        except TelegramError as exc:
            log.warning("could not notify account=%s: %s", inbound.telegram_id, exc)
