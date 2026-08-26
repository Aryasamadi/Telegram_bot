"""Acting mode: who may borrow a scope, and whose name goes in the record.

Acting mode is the one place where "what you can reach" and "who you are" come
apart. These tests pin both halves: the borrowing is refused for everyone but
the owner, and the identity underneath never moves.
"""
from __future__ import annotations

import time

import pytest

from core.acting import ActingStore
from core.context import RequestContext, Role
from core.guards import audit
from core.resolve import resolve_context
from tests.conftest import (
    ALPHA_SCOPE,
    ALPHA_TELEGRAM_ID,
    ALPHA_USER_ID,
    BETA_SCOPE,
    BETA_TELEGRAM_ID,
    OWNER_TELEGRAM_ID,
    OWNER_USER_ID,
    audit_rows,
    message,
)


async def test_manager_may_not_claim_another_scope(db):
    """A forged acting claim from a manager is refused at the boundary."""
    with pytest.raises(PermissionError):
        await resolve_context(db, ALPHA_TELEGRAM_ID, acting_manager_id=BETA_SCOPE)


async def test_manager_claiming_own_scope_is_not_acting(db):
    """Naming your own scope is a no-op, not an escalation."""
    ctx = await resolve_context(db, ALPHA_TELEGRAM_ID, acting_manager_id=ALPHA_SCOPE)
    assert ctx.real_role is Role.MANAGER
    assert ctx.effective_scope == ALPHA_SCOPE
    assert ctx.is_acting is False


async def test_owner_may_claim_any_scope(db):
    ctx = await resolve_context(db, OWNER_TELEGRAM_ID, acting_manager_id=BETA_SCOPE)
    assert ctx.real_role is Role.OWNER
    assert ctx.real_user_id == OWNER_USER_ID
    assert ctx.effective_scope == BETA_SCOPE
    assert ctx.is_acting is True


async def test_identity_survives_acting(db):
    """Borrowing a scope changes reach, never identity."""
    plain = await resolve_context(db, OWNER_TELEGRAM_ID)
    borrowed = await resolve_context(db, OWNER_TELEGRAM_ID, acting_manager_id=ALPHA_SCOPE)

    assert plain.real_user_id == borrowed.real_user_id
    assert plain.real_role is borrowed.real_role
    assert plain.effective_scope != borrowed.effective_scope


async def test_audit_names_the_owner_not_the_borrowed_scope(db):
    """Written directly, without a router, so the guarantee is unconditional."""
    ctx = RequestContext(
        real_user_id=OWNER_USER_ID,
        real_role=Role.OWNER,
        manager_id=None,
        acting_manager_id=ALPHA_SCOPE,
    )
    await audit(db, ctx, "test.action", target="42", detail="while acting")

    rows = await audit_rows(db)
    assert len(rows) == 1
    assert rows[0]["actor_user_id"] == OWNER_USER_ID
    assert rows[0]["real_role"] == "owner"
    assert rows[0]["acting_context"] == ALPHA_SCOPE
    assert rows[0]["actor_user_id"] != ALPHA_USER_ID


async def test_audit_failure_never_breaks_the_action(db):
    """If the audit write fails, the caller's operation still completes."""
    ctx = RequestContext(
        real_user_id=OWNER_USER_ID, real_role=Role.OWNER, acting_manager_id=ALPHA_SCOPE
    )

    class Broken:
        async def execute(self, sql, params=None):
            raise RuntimeError("database on fire")

    # Must not raise. Auditing is important, but it is not worth losing the
    # user's action over, and the failure is logged.
    await audit(Broken(), ctx, "test.action", target="1")


async def test_new_account_is_created_as_plain_user(db):
    """First contact stores an ordinary user, with no scope and no privileges."""
    ctx = await resolve_context(db, 999_888, display_name="Newcomer")
    assert ctx.real_role is Role.USER
    assert ctx.manager_id is None
    assert ctx.effective_scope is None
    assert ctx.is_acting is False

    row = await db.fetchone("SELECT * FROM users WHERE telegram_id = ?", [999_888])
    assert row["real_role"] == "user"


async def test_router_drops_a_claim_that_is_no_longer_permitted(db, tg, acting, router):
    """A live acting session must not survive losing the role behind it.

    This is the demotion case: the session was opened legitimately, then the
    account stopped being the owner. The claim is discarded and the request
    continues as that person really is, rather than locking them out.
    """
    acting.begin(ALPHA_TELEGRAM_ID, BETA_SCOPE)  # a claim Alpha may not hold

    await router.handle_update(message(ALPHA_TELEGRAM_ID, "/start"))

    assert acting.current(ALPHA_TELEGRAM_ID) is None
    assert tg.texts, "the request should still have been answered"


async def test_acting_sessions_expire():
    store = ActingStore(ttl_seconds=0)
    store.begin(OWNER_TELEGRAM_ID, ALPHA_SCOPE)
    time.sleep(0.01)
    assert store.current(OWNER_TELEGRAM_ID) is None


async def test_acting_store_begin_and_end():
    store = ActingStore()
    assert store.current(OWNER_TELEGRAM_ID) is None
    store.begin(OWNER_TELEGRAM_ID, ALPHA_SCOPE)
    assert store.current(OWNER_TELEGRAM_ID) == ALPHA_SCOPE
    store.end(OWNER_TELEGRAM_ID)
    assert store.current(OWNER_TELEGRAM_ID) is None
    store.end(OWNER_TELEGRAM_ID)  # ending twice is harmless


async def test_owner_bootstrap_promotes_only_the_configured_account(db, tg, acting, settings):
    """ADMIN_ID is the only path to the first owner, and it is idempotent."""
    from router import Router

    promoted_settings = type(settings)(
        telegram_bot_token=settings.telegram_bot_token,
        bot_username=settings.bot_username,
        admin_id=BETA_TELEGRAM_ID,
        cf_account_id=settings.cf_account_id,
        cf_database_id=settings.cf_database_id,
        cf_api_token=settings.cf_api_token,
        ai_api_key=settings.ai_api_key,
        ai_api_url=settings.ai_api_url,
        ai_model_name=settings.ai_model_name,
    )
    router = Router(db, tg, acting, promoted_settings)

    await router.handle_update(message(BETA_TELEGRAM_ID, "/start"))
    row = await db.fetchone("SELECT * FROM users WHERE telegram_id = ?", [BETA_TELEGRAM_ID])
    assert row["real_role"] == "owner"

    bootstraps = [r for r in await audit_rows(db) if r["action"] == "owner.bootstrap"]
    assert len(bootstraps) == 1

    # A second visit writes nothing further.
    await router.handle_update(message(BETA_TELEGRAM_ID, "/start", update_id=2))
    bootstraps = [r for r in await audit_rows(db) if r["action"] == "owner.bootstrap"]
    assert len(bootstraps) == 1

    # Alpha, who is not ADMIN_ID, is untouched.
    alpha = await db.fetchone("SELECT * FROM users WHERE telegram_id = ?", [ALPHA_TELEGRAM_ID])
    assert alpha["real_role"] == "manager"
  
