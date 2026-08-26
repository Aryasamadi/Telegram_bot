"""Where 'the owner is currently acting as manager X' is remembered.

This state is held in memory, not in the database, and that is a deliberate
decision with three consequences worth being explicit about:

1. It resets on restart. Acting mode is elevated access, so the safe default
   after any restart or redeploy is "not acting". Persisting it would mean an
   owner could be silently inside someone else's scope days later, having
   forgotten they ever entered it.
2. It expires on its own. Entering acting mode and walking away does not leave
   the door open indefinitely.
3. It costs no database writes. Cloudflare D1 bills per row written, and
   toggling a view mode is not worth paying for.

What is stored here is only a *claim*. It grants nothing by itself: it is
handed to `resolve_context`, which refuses the claim outright unless the
stored record says the caller really is the owner. A corrupted or forged value
here still cannot elevate anyone.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

log = logging.getLogger("acting")

# How long an acting session survives without being renewed, in seconds.
_DEFAULT_TTL = 2 * 60 * 60


class ActingStore:
    """Tracks which manager scope each Telegram account is currently borrowing.

    Keyed by Telegram account id rather than internal user id, because the
    router needs to look this up *before* the user record has been resolved.
    """

    def __init__(self, *, ttl_seconds: int = _DEFAULT_TTL) -> None:
        self._ttl = ttl_seconds
        # telegram_id -> (manager_id, started_at)
        self._sessions: dict[int, tuple[int, float]] = {}

    def begin(self, telegram_id: int, manager_id: int) -> None:
        """Record that this account is now acting inside `manager_id`."""
        self._sessions[int(telegram_id)] = (int(manager_id), time.time())
        log.info("acting session started: account=%s scope=%s", telegram_id, manager_id)

    def end(self, telegram_id: int) -> None:
        """Clear any acting session for this account. Safe if none exists."""
        if self._sessions.pop(int(telegram_id), None) is not None:
            log.info("acting session ended: account=%s", telegram_id)

    def current(self, telegram_id: int) -> Optional[int]:
        """The borrowed manager scope, or None if not acting or expired."""
        entry = self._sessions.get(int(telegram_id))
        if entry is None:
            return None

        manager_id, started_at = entry
        if time.time() - started_at > self._ttl:
            self._sessions.pop(int(telegram_id), None)
            log.info("acting session expired: account=%s scope=%s", telegram_id, manager_id)
            return None

        return manager_id

    def purge_expired(self) -> int:
        """Drop every expired session. Returns how many were removed.

        Sessions also expire lazily on read, so this exists only to stop the
        dictionary growing without bound in a long-running process.
        """
        cutoff = time.time() - self._ttl
        stale = [key for key, (_, started) in self._sessions.items() if started < cutoff]
        for key in stale:
            self._sessions.pop(key, None)
        return len(stale)
