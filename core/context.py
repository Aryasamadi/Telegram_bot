"""
Foundation: roles, request context, and context resolution.

Identity always comes from durable state (the stored user record), never
from the incoming payload. "Acting mode" lets an owner act within a
manager's context WITHOUT changing who they really are: real_role and
real_user_id stay fixed, and only acting_manager_id describes the borrowed
scope. When acting_manager_id is None, the caller is acting as themselves.
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
    """The resolved identity and scope for one incoming request.

    Fields:
      real_user_id      The real person making the request. Always from stored
                        state. This is what the audit trail records.
      real_role         The real person's role. Fixed for the request; never
                        raised or lowered by acting mode.
      manager_id        The caller's own resolved manager scope, or None if the
                        caller has no manager scope of their own (e.g. a plain
                        user, or an owner not tied to a single manager).
      acting_manager_id The manager scope the caller is currently borrowing, or
                        None when the caller is acting as themselves. Only an
                        owner may set this to a scope other than their own.

    is_acting is derived, so it can never disagree with acting_manager_id.
    """

    real_user_id: int
    real_role: Role
    manager_id: Optional[int] = None
    acting_manager_id: Optional[int] = None

    @property
    def is_acting(self) -> bool:
        """True when the caller is borrowing a manager scope different from their own."""
        return self.acting_manager_id is not None and self.acting_manager_id != self.manager_id

    @property
    def effective_scope(self) -> Optional[int]:
        """The manager scope actions actually apply within: borrowed if acting, else own."""
        if self.acting_manager_id is not None:
            return self.acting_manager_id
        return self.manager_id
