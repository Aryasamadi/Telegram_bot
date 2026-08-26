"""Shared test doubles and fixtures.

The database double is real SQLite, not a dictionary of canned answers. That
matters: Cloudflare D1 *is* SQLite, so the real schema and the real WHERE
clauses execute here exactly as they will in production. A test that says
"the guard refused" is therefore proving the SQL refused, not proving a mock
was configured to say no.
"""
from __future__ import annotations

import sqlite3
import time
from typing import Optional

import pytest

from config import Settings
from core.acting import ActingStore
from db.schema import apply_schema
from router import Router

OWNER_USER_ID = 1
OWNER_TELEGRAM_ID = 111
ALPHA_USER_ID = 2
ALPHA_TELEGRAM_ID = 222
BETA_USER_ID = 3
BETA_TELEGRAM_ID = 333

ALPHA_SCOPE = 1
BETA_SCOPE = 2

ALPHA_CHANNEL = 1
BETA_CHANNEL = 2
ALPHA_ITEM = 1
BETA_ITEM = 2


class FakeD1:
    """In-memory SQLite exposing the exact surface of the real D1Client."""

    def __init__(self) -> None:
        self._conn = sqlite3.connect(":memory:")
        self._conn.row_factory = sqlite3.Row

    async def query(self, sql: str, params: Optional[list] = None) -> list:
        cur = self._conn.execute(sql, list(params or []))
        return [dict(row) for row in cur.fetchall()]

    async def fetchone(self, sql: str, params: Optional[list] = None) -> Optional[dict]:
        rows = await self.query(sql, params)
        return rows[0] if rows else None

    async def execute(self, sql: str, params: Optional[list] = None) -> int:
        cur = self._conn.execute(sql, list(params or []))
        self._conn.commit()
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    async def insert_returning_id(self, sql: str, params: Optional[list] = None) -> int:
        cur = self._conn.execute(sql, list(params or []))
        self._conn.commit()
        return int(cur.lastrowid)

    async def aclose(self) -> None:
        self._conn.close()


class FakeTelegram:
    """Records outgoing calls instead of contacting Telegram."""

    def __init__(self) -> None:
        self.sent: list[tuple[int, str]] = []
        self.edited: list[tuple[int, int, str]] = []
        self.answered: list[str] = []

    async def send_message(self, chat_id, text, *, reply_markup=None):
        self.sent.append((int(chat_id), str(text)))
        return {"message_id": len(self.sent)}

    async def edit_message_text(self, chat_id, message_id, text, *, reply_markup=None):
        self.edited.append((int(chat_id), int(message_id), str(text)))
        return {"message_id": message_id}

    async def answer_callback_query(self, callback_query_id, *, text=None, show_alert=False):
        self.answered.append(str(callback_query_id))
        return True

    async def get_me(self):
        return {"username": "testbot"}

    async def aclose(self) -> None:
        return None

    @property
    def texts(self) -> list[str]:
        """Every message body shown to the caller, sent or edited."""
        return [t for _, t in self.sent] + [t for _, _, t in self.edited]


async def _seed(db: FakeD1) -> None:
    """Two independent tenants plus an owner, with nothing shared between them."""
    now = int(time.time())

    await db.execute(
        "INSERT INTO users (id, telegram_id, real_role, manager_id, display_name, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [OWNER_USER_ID, OWNER_TELEGRAM_ID, "owner", None, "Owner", now],
    )
    await db.execute(
        "INSERT INTO users (id, telegram_id, real_role, manager_id, display_name, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [ALPHA_USER_ID, ALPHA_TELEGRAM_ID, "manager", ALPHA_SCOPE, "Alpha Manager", now],
    )
    await db.execute(
        "INSERT INTO users (id, telegram_id, real_role, manager_id, display_name, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [BETA_USER_ID, BETA_TELEGRAM_ID, "manager", BETA_SCOPE, "Beta Manager", now],
    )

    for scope, name in ((ALPHA_SCOPE, "Alpha"), (BETA_SCOPE, "Beta")):
        await db.execute(
            "INSERT INTO managers (id, name, owner_user_id, enabled, created_at) "
            "VALUES (?, ?, ?, 1, ?)",
            [scope, name, OWNER_USER_ID, now],
        )

    for channel_id, scope, title in (
        (ALPHA_CHANNEL, ALPHA_SCOPE, "Alpha Channel"),
        (BETA_CHANNEL, BETA_SCOPE, "Beta Channel"),
    ):
        await db.execute(
            "INSERT INTO channels (id, manager_id, telegram_channel_id, title, enabled, created_at) "
            "VALUES (?, ?, ?, ?, 1, ?)",
            [channel_id, scope, -1000 - channel_id, title, now],
        )

    for item_id, scope, channel_id, title in (
        (ALPHA_ITEM, ALPHA_SCOPE, ALPHA_CHANNEL, "Alpha draft"),
        (BETA_ITEM, BETA_SCOPE, BETA_CHANNEL, "Beta draft"),
    ):
        await db.execute(
            "INSERT INTO queue (id, manager_id, channel_id, title, body, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, 'draft', ?)",
            [item_id, scope, channel_id, title, f"body of {title}", now],
        )


@pytest.fixture
async def db():
    fake = FakeD1()
    await apply_schema(fake)
    await _seed(fake)
    yield fake
    await fake.aclose()


@pytest.fixture
def tg():
    return FakeTelegram()


@pytest.fixture
def acting():
    return ActingStore()


@pytest.fixture
def settings():
    # admin_id is None so the owner-bootstrap path cannot interfere with tests
    # that are about something else. One test sets it explicitly.
    return Settings(
        telegram_bot_token="test-token",
        bot_username="testbot",
        admin_id=None,
        cf_account_id="test-account",
        cf_database_id="test-database",
        cf_api_token="test-token",
        ai_api_key=None,
        ai_api_url="https://example.invalid/v1",
        ai_model_name="test-model",
    )


@pytest.fixture
def router(db, tg, acting, settings):
    return Router(db, tg, acting, settings)


def message(telegram_id: int, text: str, *, update_id: int = 1) -> dict:
    """A raw Telegram text-message update."""
    return {
        "update_id": update_id,
        "message": {
            "message_id": 100,
            "from": {"id": telegram_id, "first_name": "Tester"},
            "chat": {"id": telegram_id},
            "text": text,
        },
    }


def press(telegram_id: int, data: str, *, update_id: int = 1) -> dict:
    """A raw Telegram button-press update carrying arbitrary callback data."""
    return {
        "update_id": update_id,
        "callback_query": {
            "id": f"cbq-{update_id}",
            "from": {"id": telegram_id, "first_name": "Tester"},
            "message": {"message_id": 100, "chat": {"id": telegram_id}},
            "data": data,
        },
    }


async def audit_rows(db: FakeD1) -> list:
    return await db.query("SELECT * FROM audit ORDER BY id")


async def queue_row(db: FakeD1, item_id: int) -> dict:
    return await db.fetchone("SELECT * FROM queue WHERE id = ?", [item_id])
