"""Entry point: start up, verify everything, then poll Telegram until stopped.

Startup order is deliberate, and each step fails loudly rather than limping on:

1. Load configuration. Missing required variables stop the process immediately
   with a message naming every one of them, so a misconfigured deploy is
   obvious in the logs instead of producing a bot that silently does nothing.
2. Connect to the database and apply the schema. Applying it on every start is
   what removes the need to ever run SQL by hand: the statements create tables
   only if absent, so a first deploy and the hundredth behave identically.
3. Verify the bot token with Telegram before entering the loop. A bad token
   should be reported at second three, not discovered later.

The polling loop then survives network trouble on its own, backing off when
Telegram is unreachable and resuming when it returns. It shuts down cleanly on
SIGTERM, which is the signal Railway sends when redeploying.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys
from typing import Optional

from config import ConfigError, Settings, load_settings
from core.acting import ActingStore
from db.d1 import D1Client, D1Error
from db.schema import apply_schema
from infra.telegram import TelegramClient, TelegramError
from router import Router

log = logging.getLogger("main")

# Backoff bounds when polling fails, in seconds.
_BACKOFF_START = 5
_BACKOFF_MAX = 60

# How many polling rounds between sweeps of expired acting sessions.
_PURGE_INTERVAL = 100


def _configure_logging() -> None:
    """Log to stdout, which is what Railway captures and displays."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    # httpx logs every HTTP request at INFO, which would bury everything else
    # and echo request URLs into the log.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def _install_signal_handlers(stop: asyncio.Event) -> None:
    """Ask the loop to stop on SIGTERM or SIGINT, rather than dying mid-request."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            # Not available on every platform; the process will still exit, just
            # less gracefully.
            log.debug("signal handler unavailable for %s", sig)


async def _startup(
    settings: Settings,
) -> tuple[D1Client, TelegramClient, ActingStore, Router]:
    """Build every component and prove it works. Raises on any failure."""
    log.info("configuration: %s", settings.describe())

    db = D1Client(
        settings.cf_account_id,
        settings.cf_database_id,
        settings.cf_api_token,
    )

    log.info("applying database schema")
    await apply_schema(db)

    tg = TelegramClient(settings.telegram_bot_token)
    identity = await tg.get_me()
    username = identity.get("username") or "unknown"
    log.info("connected to Telegram as @%s", username)

    acting = ActingStore()
    router = Router(db, tg, acting, settings)
    return db, tg, acting, router


async def _poll_forever(
    tg: TelegramClient,
    router: Router,
    acting: ActingStore,
    stop: asyncio.Event,
) -> None:
    """Long-poll Telegram and route each update until asked to stop.

    Updates are handled one at a time. That is slower than running them
    concurrently, but it means two taps from the same person cannot interleave
    halfway through a database write, and the ordering a person sees matches the
    order they acted. Throughput is not the constraint for this workload.
    """
    offset: Optional[int] = None
    backoff = _BACKOFF_START
    rounds = 0

    log.info("polling started")
    while not stop.is_set():
        try:
            updates = await tg.get_updates(offset)
            backoff = _BACKOFF_START  # a good poll resets the penalty
        except TelegramError as exc:
            log.warning("polling failed, retrying in %ss: %s", backoff, exc)
            try:
                await asyncio.wait_for(stop.wait(), timeout=backoff)
                break  # stop was requested while waiting
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, _BACKOFF_MAX)
            continue

        for update in updates:
            update_id = update.get("update_id")
            if isinstance(update_id, int):
                # Confirming the offset tells Telegram not to resend this one.
                # It is advanced before handling so a message that reliably
                # crashes cannot be redelivered forever in a loop.
                offset = update_id + 1
            await router.handle_update(update)

        rounds += 1
        if rounds % _PURGE_INTERVAL == 0:
            removed = acting.purge_expired()
            if removed:
                log.info("purged %d expired acting session(s)", removed)

    log.info("polling stopped")


async def run() -> int:
    """Start everything, poll until stopped, then shut down. Returns exit code."""
    stop = asyncio.Event()
    _install_signal_handlers(stop)

    try:
        settings = load_settings()
    except ConfigError as exc:
        log.error("cannot start: %s", exc)
        return 1

    db: Optional[D1Client] = None
    tg: Optional[TelegramClient] = None
    try:
        db, tg, acting, router = await _startup(settings)
    except D1Error as exc:
        log.error("cannot start: database unreachable or rejected: %s", exc)
        return 1
    except TelegramError as exc:
        log.error("cannot start: Telegram rejected the bot token: %s", exc)
        return 1
    except ValueError as exc:
        log.error("cannot start: %s", exc)
        return 1
    finally:
        # If startup failed after a client was created, close it rather than
        # leaking the connection pool on the way out.
        if tg is None and db is not None:
            await db.aclose()

    try:
        await _poll_forever(tg, router, acting, stop)
    finally:
        await tg.aclose()
        await db.aclose()
        log.info("shutdown complete")

    return 0


def main() -> None:
    _configure_logging()
    try:
        code = asyncio.run(run())
    except KeyboardInterrupt:
        code = 0
    sys.exit(code)


if __name__ == "__main__":
    main()
