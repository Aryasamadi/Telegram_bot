"""Provider failover and JSON extraction.

The rule these tests defend: when every provider fails, the chain reports the
failure. It never returns an empty-but-successful result, because a caller that
believes it received content will happily publish nothing at all.
"""
from __future__ import annotations

import pytest

from ai.providers import AIResult, ProviderChain, _extract_json


class Provider:
    """A scripted provider. `outcome` is a result, an exception, or None."""

    def __init__(self, name, outcome):
        self.name = name
        self._outcome = outcome
        self.calls = 0

    async def complete(self, system, user, expect_json=False):
        self.calls += 1
        if isinstance(self._outcome, Exception):
            raise self._outcome
        return self._outcome


def ok(data=None, provider=None):
    return AIResult(ok=True, data=data or {"title": "t", "body": "b"}, provider=provider)


def fail(error="upstream refused"):
    return AIResult(ok=False, error=error)


async def test_empty_chain_is_rejected_at_construction():
    with pytest.raises(ValueError):
        ProviderChain([])


async def test_first_success_wins_and_later_providers_are_untouched():
    first = Provider("primary", ok())
    second = Provider("backup", ok())

    result = await ProviderChain([first, second]).run("sys", "usr")

    assert result.ok
    assert result.provider == "primary"
    assert second.calls == 0


async def test_failover_moves_to_the_next_provider():
    first = Provider("primary", fail())
    second = Provider("backup", ok())

    result = await ProviderChain([first, second]).run("sys", "usr")

    assert result.ok
    assert result.provider == "backup"
    assert first.calls == 1


async def test_a_crashing_provider_does_not_stop_failover():
    """An exception is a failure like any other, not the end of the attempt."""
    first = Provider("primary", RuntimeError("connection reset"))
    second = Provider("backup", ok())

    result = await ProviderChain([first, second]).run("sys", "usr")

    assert result.ok
    assert result.provider == "backup"


async def test_a_provider_returning_none_does_not_stop_failover():
    first = Provider("primary", None)
    second = Provider("backup", ok())

    result = await ProviderChain([first, second]).run("sys", "usr")

    assert result.ok
    assert result.provider == "backup"


async def test_total_failure_reports_an_error_and_no_data():
    first = Provider("primary", fail("quota exhausted"))
    second = Provider("backup", RuntimeError("timed out"))

    result = await ProviderChain([first, second]).run("sys", "usr")

    assert result.ok is False
    assert result.data is None
    assert result.error
    assert "timed out" in result.error
    assert result.provider == "backup"


async def test_existing_provider_stamp_is_preserved():
    """A provider that names itself is not overwritten by the chain."""
    result = await ProviderChain([Provider("chain-name", ok(provider="self-named"))]).run("s", "u")
    assert result.provider == "self-named"


def test_extract_json_plain_object():
    assert _extract_json('{"a": 1}') == {"a": 1}


def test_extract_json_ignores_surrounding_prose():
    raw = 'Certainly! Here you go:\n```json\n{"title": "x", "body": "y"}\n```\nHope that helps.'
    assert _extract_json(raw) == {"title": "x", "body": "y"}


def test_extract_json_tolerates_braces_inside_strings():
    """A `}` inside a string literal must not be read as the end of the object.

    This is the case naive brace-counting gets wrong, and models produce it
    constantly the moment an article mentions code or JSON.
    """
    raw = '{"body": "the function returns {} when empty", "title": "Braces"}'
    assert _extract_json(raw) == {
        "body": "the function returns {} when empty",
        "title": "Braces",
    }


def test_extract_json_tolerates_escaped_quotes():
    raw = '{"body": "he said \\"hello\\" and left"}'
    assert _extract_json(raw) == {"body": 'he said "hello" and left'}


def test_extract_json_returns_none_for_unusable_input():
    assert _extract_json("") is None
    assert _extract_json("no json at all") is None
    assert _extract_json("{not valid json}") is None
    assert _extract_json('["a", "list"]') is None  # a list is not an object
