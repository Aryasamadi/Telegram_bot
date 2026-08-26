"""The highest-priority security tests: one tenant must never reach another.

Every test here drives `Router.handle_update` with raw Telegram payloads, which
is the same entry point production uses. Nothing is called past the router, so
there is no way for a test to accidentally skip a check that a real request
would have to pass.

The callback data in these tests is forged by hand. That is not an exotic
attack: callback data is just a string in an API request, entirely under the
sender's control. Any design that trusts it is already broken.
"""
from __future__ import annotations

import pytest

from core.context import RequestContext, Role
from core.guards import (
    OwnershipError,
    require_channel_owned,
    require_queue_item_owned,
)
from tests.conftest import (
    ALPHA_CHANNEL,
    ALPHA_ITEM,
    ALPHA_SCOPE,
    ALPHA_TELEGRAM_ID,
    BETA_CHANNEL,
    BETA_ITEM,
    BETA_SCOPE,
    BETA_TELEGRAM_ID,
    OWNER_TELEGRAM_ID,
    OWNER_USER_ID,
    audit_rows,
    press,
    queue_row,
)

REFUSED = "That isn't available."


async def test_owner_acting_cannot_reach_foreign_tenant(db, tg, acting, router):
    """The six-point requirement, in order.

    1. The owner enters acting mode for Alpha.
    2. The owner forges a button pointing at Beta's queue item.
    3. The ownership guard must reject it.
    4. Beta's data must be untouched.
    5. The audit trail must not record a successful action on Beta's item.
    6. (Covered by the next test: the legitimate path still works.)
    """
    acting.begin(OWNER_TELEGRAM_ID, ALPHA_SCOPE)

    before = await queue_row(db, BETA_ITEM)
    assert before["status"] == "draft"

    await router.handle_update(press(OWNER_TELEGRAM_ID, f"m:item:{BETA_ITEM}"))

    # 3. Rejected, and told nothing about why or about what exists.
    assert REFUSED in tg.texts
    assert "Beta" not in " ".join(tg.texts)

    # 4. Beta's row is byte-for-byte unchanged.
    after = await queue_row(db, BETA_ITEM)
    assert after == before

    # 5. No audit row claims an action against Beta's item.
    rows = await audit_rows(db)
    assert not [r for r in rows if r["target"] == str(BETA_ITEM)]


async def test_owner_acting_publish_records_real_owner_id(db, tg, acting, router):
    """The legitimate acting path works, and names the owner — not the manager.

    This is the other half of the requirement: isolation is only meaningful if
    valid operations still succeed, and acting mode is only auditable if the
    row identifies the real person behind the borrowed scope.
    """
    acting.begin(OWNER_TELEGRAM_ID, ALPHA_SCOPE)

    await router.handle_update(press(OWNER_TELEGRAM_ID, f"m:publish:{ALPHA_ITEM}"))

    item = await queue_row(db, ALPHA_ITEM)
    assert item["status"] == "published"
    assert item["published_at"] is not None

    # The post reached Alpha's channel and nowhere else.
    assert len(tg.sent) >= 1
    assert any(chat_id == -1000 - ALPHA_CHANNEL for chat_id, _ in tg.sent)

    published = [r for r in await audit_rows(db) if r["action"] == "queue.publish"]
    assert len(published) == 1
    row = published[0]

    # The actor is the owner's own user id, never Alpha's.
    assert row["actor_user_id"] == OWNER_USER_ID
    assert row["real_role"] == "owner"
    # The borrowed scope is recorded separately, so both facts survive.
    assert row["acting_context"] == ALPHA_SCOPE


async def test_manager_cannot_reach_another_managers_item(db, tg, router):
    """A manager forging another tenant's item id gets the same refusal."""
    before = await queue_row(db, BETA_ITEM)

    await router.handle_update(press(ALPHA_TELEGRAM_ID, f"m:item:{BETA_ITEM}"))

    assert REFUSED in tg.texts
    assert await queue_row(db, BETA_ITEM) == before


async def test_manager_cannot_toggle_another_managers_channel(db, tg, router):
    """A write against a foreign channel must change nothing."""
    before = await db.fetchone("SELECT * FROM channels WHERE id = ?", [BETA_CHANNEL])

    await router.handle_update(press(ALPHA_TELEGRAM_ID, f"m:chtoggle:{BETA_CHANNEL}"))

    assert REFUSED in tg.texts
    after = await db.fetchone("SELECT * FROM channels WHERE id = ?", [BETA_CHANNEL])
    assert after == before


async def test_manager_cannot_open_owner_surface(db, tg, router):
    """The owner namespace is unreachable for a manager, and reveals nothing."""
    await router.handle_update(press(ALPHA_TELEGRAM_ID, "o:managers"))

    assert REFUSED in tg.texts
    # No manager names leaked into the reply.
    joined = " ".join(tg.texts)
    assert "Alpha" not in joined and "Beta" not in joined


async def test_manager_cannot_publish_another_managers_item(db, tg, router):
    """The publish path is guarded before anything is sent to Telegram."""
    await router.handle_update(press(ALPHA_TELEGRAM_ID, f"m:publish:{BETA_ITEM}"))

    assert REFUSED in tg.texts
    assert (await queue_row(db, BETA_ITEM))["status"] == "draft"
    # Crucially: nothing was posted to any channel.
    assert not [c for c, _ in tg.sent if c < 0]


async def test_guard_rejects_foreign_ids_directly(db):
    """The guards refuse on their own, independent of any routing decision.

    Routing could be rewritten tomorrow; these two functions are the floor that
    every path stands on, so they are tested without a router in the way.
    """
    alpha_ctx = RequestContext(
        real_user_id=OWNER_USER_ID,
        real_role=Role.OWNER,
        manager_id=None,
        acting_manager_id=ALPHA_SCOPE,
    )

    with pytest.raises(OwnershipError):
        await require_queue_item_owned(db, alpha_ctx, BETA_ITEM)
    with pytest.raises(OwnershipError):
        await require_channel_owned(db, alpha_ctx, BETA_CHANNEL)

    # Same context, own scope: allowed.
    assert (await require_queue_item_owned(db, alpha_ctx, ALPHA_ITEM))["id"] == ALPHA_ITEM
    assert (await require_channel_owned(db, alpha_ctx, ALPHA_CHANNEL))["id"] == ALPHA_CHANNEL


async def test_guard_refuses_when_no_scope_at_all(db):
    """An owner who is not acting has no manager scope, so scoped reads fail."""
    bare_ctx = RequestContext(
        real_user_id=OWNER_USER_ID,
        real_role=Role.OWNER,
        manager_id=None,
        acting_manager_id=None,
    )
    with pytest.raises(OwnershipError):
        await require_queue_item_owned(db, bare_ctx, ALPHA_ITEM)


async def test_nonexistent_id_is_indistinguishable_from_forbidden(db, tg, router):
    """A missing id and a forbidden id must produce the identical reply.

    If they differed, anyone could enumerate which ids exist by watching which
    message came back.
    """
    await router.handle_update(press(ALPHA_TELEGRAM_ID, "m:item:999999"))
    missing = list(tg.texts)

    tg.sent.clear()
    tg.edited.clear()
    await router.handle_update(press(ALPHA_TELEGRAM_ID, f"m:item:{BETA_ITEM}"))
    forbidden = list(tg.texts)

    assert missing == forbidden == [REFUSED]
