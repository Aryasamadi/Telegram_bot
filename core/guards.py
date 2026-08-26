"""Ownership guards and the audit trail.

Two security invariants live here:

  1. Ownership is always scoped by the ACTING context (acting_manager_id when
     an owner is acting inside a manager's world, otherwise the real manager
     scope). It is NEVER trusted from the incoming payload. A guard that let
     the caller name their own scope would defeat tenant isolation entirely.

  2. The audit trail always records the REAL actor (real_user_id) — never the
     borrowed identity. "Owner X acting as manager Y did Z" must remain
     attributable to X.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from core.context import RequestContext, Role

log = logging.getLogger("guards")


class OwnershipError(PermissionError):
    """Raised when a caller tries to act on a resource outside their scope."""


def _acting_scope(ctx: RequestContext) -> int:
    """The manager scope the caller is currently acting within.

    If the owner is acting inside a manager's context, that manager's id is the
    scope. Otherwise the caller's own resolved manager scope applies. This value
    comes only from resolved context, never from request payloads.
    """
    if ctx.acting_manager_id is not None:
        return ctx.acting_manager_id
    if ctx.manager_id is not None:
        return ctx.manager_id
    raise OwnershipError("no manager scope in context")


async def require_channel_owned(db, ctx: RequestContext, channel_id: int) -> dict:
    """Load a channel and confirm it belongs to the acting manager scope."""
    scope = _acting_scope(ctx)
    row = await db.fetchone(
        "SELECT * FROM channels WHERE id = ? AND manager_id = ?",
        [channel_id, scope],
    )
    if row is None:
        raise OwnershipError(f"channel {channel_id} not owned by scope {scope}")
    return row


async def require_queue_item_owned(db, ctx: RequestContext, item_id: int) -> dict:
    """Load a queue item and confirm it belongs to the acting manager scope."""
    scope = _acting_scope(ctx)
    row = await db.fetchone(
        "SELECT * FROM queue WHERE id = ? AND manager_id = ?",
        [item_id, scope],
    )
    if row is None:
        raise OwnershipError(f"queue item {item_id} not owned by scope {scope}")
    return row
  

async def require_manager_owned(db, ctx: RequestContext, manager_id: int) -> dict:
    """Confirm the caller may administer this manager.

    Only an owner may target a manager other than their own acting scope; a
    manager is confined to their own scope.
    """
    if ctx.real_role == Role.OWNER:
        row = await db.fetchone("SELECT * FROM managers WHERE id = ?", [manager_id])
    else:
        scope = _acting_scope(ctx)
        if manager_id != scope:
            raise OwnershipError(
                f"manager {manager_id} outside caller scope {scope}"
            )
        row = await db.fetchone("SELECT * FROM managers WHERE id = ?", [manager_id])
    if row is None:
        raise OwnershipError(f"manager {manager_id} not found")
    return row


async def audit(
    db,
    ctx: RequestContext,
    action: str,
    *,
    target: Optional[str] = None,
    detail: Optional[str] = None,
) -> None:
    """Write one audit row recording the REAL actor and the acting context.

    actor_user_id  -> the real person (never the borrowed identity)
    acting_context -> the manager scope the action happened within, if any
    """
    acting_context = ctx.acting_manager_id if ctx.acting_manager_id is not None else ctx.manager_id
    try:
        await db.execute(
            "INSERT INTO audit "
            "(actor_user_id, real_role, acting_context, action, target, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                ctx.real_user_id,
                ctx.real_role.value if isinstance(ctx.real_role, Role) else str(ctx.real_role),
                acting_context,
                action,
                target,
                detail,
                int(time.time()),
            ],
        )
    except Exception as exc:  # audit must never crash the caller's action
        log.error("audit write failed for action=%s actor=%s: %s", action, ctx.real_user_id, exc)
