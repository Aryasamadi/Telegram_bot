"""Database schema, applied automatically at startup.

Every statement is idempotent (IF NOT EXISTS), so apply_schema can run on
every boot without destroying or duplicating anything. There is no manual SQL
step: main.py calls apply_schema() during startup.

Column names here are the single source of truth. core/guards.py and the
service layer are written against exactly these names.
"""
from __future__ import annotations

import logging

log = logging.getLogger("db.schema")

SCHEMA_STATEMENTS: list[str] = [
    # --- people ---
    """
    CREATE TABLE IF NOT EXISTS users (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        telegram_id   INTEGER NOT NULL UNIQUE,
        real_role     TEXT    NOT NULL DEFAULT 'user',
        manager_id    INTEGER,
        display_name  TEXT,
        created_at    INTEGER NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_users_telegram ON users (telegram_id)",

    # --- tenants ---
    """
    CREATE TABLE IF NOT EXISTS managers (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        name           TEXT    NOT NULL,
        owner_user_id  INTEGER,
        enabled        INTEGER NOT NULL DEFAULT 1,
        created_at     INTEGER NOT NULL
    )
    """,

    # --- channels, scoped to a tenant ---
    """
    CREATE TABLE IF NOT EXISTS channels (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        manager_id           INTEGER NOT NULL,
        telegram_channel_id  TEXT    NOT NULL,
        title                TEXT,
        enabled              INTEGER NOT NULL DEFAULT 1,
        created_at           INTEGER NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_channels_manager ON channels (manager_id)",

    # --- content queue, scoped to a tenant ---
    """
    CREATE TABLE IF NOT EXISTS queue (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        manager_id    INTEGER NOT NULL,
        channel_id    INTEGER NOT NULL,
        title         TEXT,
        body          TEXT,
        status        TEXT    NOT NULL DEFAULT 'draft',
        source_text   TEXT,
        provider      TEXT,
        error         TEXT,
        created_at    INTEGER NOT NULL,
        published_at  INTEGER
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_queue_manager ON queue (manager_id)",
    "CREATE INDEX IF NOT EXISTS idx_queue_status ON queue (manager_id, status)",

    # --- audit trail: real actor, borrowed scope ---
    """
    CREATE TABLE IF NOT EXISTS audit (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        actor_user_id   INTEGER NOT NULL,
        real_role       TEXT    NOT NULL,
        acting_context  INTEGER,
        action          TEXT    NOT NULL,
        target          TEXT,
        detail          TEXT,
        created_at      INTEGER NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_audit_actor ON audit (actor_user_id)",
    "CREATE INDEX IF NOT EXISTS idx_audit_created ON audit (created_at)",
]


async def apply_schema(db) -> None:
    """Run every schema statement in order. Safe to call on every startup."""
    for statement in SCHEMA_STATEMENTS:
        sql = " ".join(statement.split())
        await db.execute(sql)
    log.info("schema applied: %d statements", len(SCHEMA_STATEMENTS))
