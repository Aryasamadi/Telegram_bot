"""Context resolution: turn an incoming Telegram user id into a RequestContext.

Identity is read from durable state (the users table), never from the incoming
payload. A first-time user is created with the lowest role.

Acting mode is validated HERE, at the boundary: only an owner may borrow a
manager scope other than their own. A non-owner attempting it is refused
outright rather than silently downgraded, so the attempt surfaces instead of
passing quietly.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from core.context import RequestContext, Role

log = logging.getLogger("core.resolve")


async def _load_or_create_user(
    db, telegram_id: int, display_name: Optional[str]
) -> dict:
    """Fetch the stored user row, creating it on first contact."""
    row = await db.fetchone("SELECT * FROM users WHERE telegram_id = ?", [telegram_id])
    if row is not None:
        return row
    await db.execute(
        "INSERT INTO users (telegram_id, real_role, display_name, created_at) "
        "VALUES (?, ?, ?, ?)",
        [telegram_id, Role.USER.value, display_name, int(time.time())],
    )
    row = await db.fetchone("SELECT * FROM users WHERE telegram_id = ?", [telegram_id])
    if row is None:
        raise RuntimeError(f"failed to create user for telegram_id {telegram_id}")
    log.info("created user telegram_id=%s", telegram_id)
    return row


def _coerce_role(raw) -> Role:
    """Map a stored role string to a Role, defaulting safely on bad data."""
    try:
        return Role(str(raw))
    except ValueError:
        log.warning("unknown role %r in users row; defaulting to user", raw)
        return Role.USER


async def resolve_context(
    db,
    telegram_id: int,
    *,
    display_name: Optional[str] = None,
    acting_manager_id: Optional[int] = None,
) -> RequestContext:
    """Build the resolved context for one incoming request.

    acting_manager_id is the scope the caller is asking to act within. It is
    accepted only for an owner; any other role requesting a scope that is not
    their own is refused with PermissionError.
    """
    row = await _load_or_create_user(db, telegram_id, display_name)

    real_role = _coerce_role(row.get("real_role"))

    raw_scope = row.get("manager_id")
    own_scope = int(raw_scope) if isinstance(raw_scope, int) else None

    if acting_manager_id is not None and acting_manager_id != own_scope:
        if real_role != Role.OWNER:
            log.warning(
                "refused acting request: user=%s role=%s asked for scope=%s (own=%s)",
                row.get("id"),
                real_role.value,
                acting_manager_id,
                own_scope,
            )
            raise PermissionError(
                f"role {real_role.value} may not act within manager scope {acting_manager_id}"
            )

    return RequestContext(
        real_user_id=int(row["id"]),
        real_role=real_role,
        manager_id=own_scope,
        acting_manager_id=acting_manager_id,
    )
