"""Cloudflare D1 database client over the HTTP API.

D1 is normally reached from inside Cloudflare Workers. From an external host
(e.g. Railway) it is reached through the D1 REST API using an account ID, a
database ID, and an API token. This client wraps that endpoint and exposes a
small, synchronous-feeling async surface the services rely on:

    query(sql, params)          -> list[dict]   (rows)
    execute(sql, params)        -> int          (rows affected)
    fetchone(sql, params)       -> dict | None
    insert_returning_id(sql, params) -> int
    aclose()                    -> None         (release the connection)

One HTTP connection is kept open and reused for every statement. Opening a new
one per query would mean a fresh TLS handshake to Cloudflare every time, which
is the dominant cost when a single screen runs several small queries. Whoever
creates this client is responsible for calling `aclose()` on shutdown.

All methods raise D1Error on transport or API failure so callers can fail
cleanly rather than acting on partial data.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

log = logging.getLogger("db")

_API_ROOT = "https://api.cloudflare.com/client/v4"


class D1Error(RuntimeError):
    """Raised when D1 returns an error or the request cannot be completed."""


class D1Client:
    def __init__(
        self,
        account_id: str,
        database_id: str,
        api_token: str,
        *,
        timeout: float = 15.0,
    ) -> None:
        if not account_id or not database_id or not api_token:
            raise ValueError("D1Client requires account_id, database_id, and api_token")
        self._url = (
            f"{_API_ROOT}/accounts/{account_id}/d1/database/{database_id}/query"
        )
        self._headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        }
        self._timeout = timeout
        self._client = httpx.AsyncClient(timeout=timeout)

    async def aclose(self) -> None:
        """Release the underlying HTTP connection. Safe to call more than once."""
        await self._client.aclose()

    async def _raw_query(self, sql: str, params: Optional[list] = None) -> dict:
        """Send one SQL statement to D1 and return the first result block."""
        payload = {"sql": sql, "params": list(params or [])}
        try:
            resp = await self._client.post(
                self._url, headers=self._headers, json=payload
            )
        except httpx.HTTPError as exc:
            raise D1Error(f"transport error: {exc}") from exc

        if resp.status_code != 200:
            raise D1Error(f"http {resp.status_code}: {resp.text[:300]}")

        try:
            body = resp.json()
        except ValueError as exc:
            raise D1Error(f"invalid JSON from D1: {exc}") from exc

        if not body.get("success", False):
            errors = body.get("errors") or []
            raise D1Error(f"D1 API error: {errors}")

        result = body.get("result") or []
        if not result:
            return {"results": [], "meta": {}}
        # D1 returns a list of result blocks (one per statement); we send one.
        return result[0]

    async def query(self, sql: str, params: Optional[list] = None) -> list:
        """Run a SELECT and return all rows as a list of dicts."""
        block = await self._raw_query(sql, params)
        rows = block.get("results")
        return rows if isinstance(rows, list) else []

    async def fetchone(self, sql: str, params: Optional[list] = None) -> Optional[dict]:
        """Run a SELECT and return the first row as a dict, or None."""
        rows = await self.query(sql, params)
        return rows[0] if rows else None

    async def execute(self, sql: str, params: Optional[list] = None) -> int:
        """Run an INSERT/UPDATE/DELETE and return the number of rows changed."""
        block = await self._raw_query(sql, params)
        meta = block.get("meta") or {}
        changes = meta.get("changes")
        return int(changes) if isinstance(changes, int) else 0

    async def insert_returning_id(self, sql: str, params: Optional[list] = None) -> int:
        """Run an INSERT and return the new row's integer primary key."""
        block = await self._raw_query(sql, params)
        meta = block.get("meta") or {}
        row_id = meta.get("last_row_id")
        if not isinstance(row_id, int):
            raise D1Error("insert did not return a last_row_id")
        return row_id
