"""Verification policy: `resolve_verdict`, branch by branch.

These tests call the production function directly rather than reimplementing
its logic, so a change to the branch order shows up here as a failure instead
of quietly diverging from a copy.

The policy worth understanding: an unreachable verifier passes the draft
through (fail open), while a verifier that positively reports unsupported
claims and offers no correction holds the post (fail closed). Those are
opposite defaults, and the difference is deliberate — an outage must not stop
publishing, but a stated objection must.
"""
from __future__ import annotations

from ai.providers import AIResult, resolve_verdict


def draft(body="original body"):
    return AIResult(ok=True, data={"title": "t", "body": body}, provider="primary")


def verdict(**data):
    return AIResult(ok=True, data=data, provider="verifier")


def test_disabled_returns_the_draft_untouched():
    prior = draft()
    assert resolve_verdict(prior, verdict(supported=False), enabled=False) is prior


def test_missing_verdict_fails_open():
    prior = draft()
    assert resolve_verdict(prior, None, enabled=True) is prior


def test_failed_verdict_fails_open():
    """The verifier itself errored: an outage must not block the pipeline."""
    prior = draft()
    unreachable = AIResult(ok=False, error="verifier timed out")
    assert resolve_verdict(prior, unreachable, enabled=True) is prior


def test_ok_verdict_with_no_data_fails_open():
    """Reported success but returned nothing usable — still an outage."""
    prior = draft()
    empty = AIResult(ok=True, data=None, provider="verifier")
    assert resolve_verdict(prior, empty, enabled=True) is prior


def test_supported_returns_the_draft_untouched():
    prior = draft()
    assert resolve_verdict(prior, verdict(supported=True), enabled=True) is prior


def test_correction_replaces_the_body_and_keeps_the_title():
    prior = draft()
    result = resolve_verdict(
        prior, verdict(supported=False, corrected_body="fixed body"), enabled=True
    )

    assert result.ok
    assert result.data["body"] == "fixed body"
    assert result.data["title"] == "t"
    assert result.provider == "primary"
    # The original result object is not mutated.
    assert prior.data["body"] == "original body"


def test_unsupported_without_a_correction_holds_the_post():
    result = resolve_verdict(draft(), verdict(supported=False), enabled=True)

    assert result.ok is False
    assert "unsupported" in result.error


def test_empty_string_correction_holds_the_post():
    """An empty correction is not a correction, and must not publish nothing."""
    result = resolve_verdict(
        draft(), verdict(supported=False, corrected_body=""), enabled=True
    )
    assert result.ok is False


def test_null_correction_holds_the_post():
    result = resolve_verdict(
        draft(), verdict(supported=False, corrected_body=None), enabled=True
    )
    assert result.ok is False


def test_missing_supported_key_is_treated_as_not_supported():
    """Absence is not consent: a verdict that never says True must not pass."""
    result = resolve_verdict(draft(), verdict(note="malformed"), enabled=True)
    assert result.ok is False
