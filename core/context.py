"""
Foundation: roles, request context, and context resolution.

Identity always comes from durable state (the stored user record), never
from the incoming payload. "Acting mode" lets an owner act within a
manager's context WITHOUT changing who they really are — real_role and
real_user_id stay fixed; only the acting_* fields describe the borrowed
context.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Role(str, Enum):
    USER = "user"
    MANAGER = "manager"
    OWNER = "owner"


@dataclass(frozen=True)
class RequestContext:
    """
    The resolved identity and context for a single incoming update.

    Fields:
      real_user_id   -- durable id of the human making the request (from state)
      real_role      -- that human's true role (from state, never the payload)
      acting_role    -- the role whose context is currently in effect
      manager_id     -- the tenant/manager whose data this request operates on
      is_acting      -- True when an owner is operating inside another context
    """
    real_user_id: int
    real_role: Role
    acting_role: Role
    manager_id: Optional[int]
    is_acting: bool

    @property
    def effective_role(self) -> Role:
        """The role that governs what this request is allowed to do."""
        return self.acting_role

    def scoped_to(self, manager_id: int) -> "RequestContext":
        """
        Return a copy operating inside the given manager's context.

        Only an owner may enter acting mode. real_user_id and real_role are
        preserved exactly; the borrowed context lives only in acting_role,
        manager_id, and is_acting.
        """
        if self.real_role is not Role.OWNER:
            raise PermissionError("only an owner may act within another context")
        return RequestContext(
            real_user_id=self.real_user_id,
            real_role=self.real_role,
            acting_role=Role.MANAGER,
            manager_id=manager_id,
            is_acting=True,
        )


async def resolve_context(user_id: int, state) -> Optional[RequestContext]:
    """
    Build a RequestContext from DURABLE STATE, not from the payload.

    `state` is any object exposing an async get_user(user_id) that returns a
    record with `role` (a Role or its string value) and, for managers, an
    optional `manager_id`. Returns None for unknown users so the caller can
    fail closed.
    """
    record = await state.get_user(user_id)
    if record is None:
        return None

    raw_role = record["role"] if isinstance(record, dict) else record.role
    role = raw_role if isinstance(raw_role, Role) else Role(raw_role)

    if isinstance(record, dict):
        manager_id = record.get("manager_id")
    else:
        manager_id = getattr(record, "manager_id", None)

    # A manager's own tenant is themselves when no explicit manager_id is set.
    if role is Role.MANAGER and manager_id is None:
        manager_id = user_id

    return RequestContext(
        real_user_id=user_id,
        real_role=role,
        acting_role=role,
        manager_id=manager_id,
        is_acting=False,
    )
