"""AI provider layer: result type, failover chain, JSON extraction, verdict policy."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger("ai")


@dataclass
class AIResult:
    ok: bool
    data: Optional[dict] = None
    text: Optional[str] = None
    error: Optional[str] = None
    provider: Optional[str] = None


def _extract_json(raw: str) -> Optional[dict]:
    """Pull one JSON object from a model response; string-aware; None if nothing parses."""
    if not raw:
        return None
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except (ValueError, TypeError):
        pass
    start = raw.find("{")
    if start == -1:
        return None
    depth = 0
    in_string = False
    escaped = False
    for i in range(start, len(raw)):
        ch = raw[i]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = raw[start:i + 1]
                try:
                    obj = json.loads(candidate)
                    return obj if isinstance(obj, dict) else None
                except (ValueError, TypeError):
                    return None
    return None


class ProviderChain:
    """Runs providers in order until one succeeds. Never fabricates on total failure."""

    def __init__(self, providers: list) -> None:
        if not providers:
            raise ValueError("ProviderChain requires at least one provider")
        self._providers = providers

    async def run(self, system: str, user: str, expect_json: bool = False) -> AIResult:
        """Try each provider in turn; return the first ok result, else the last error."""
        last_error = "no providers ran"
        last_provider = None
        for provider in self._providers:
            name = getattr(provider, "name", provider.__class__.__name__)
            try:
                result = await provider.complete(system, user, expect_json)
            except Exception as exc:  # a crashing provider must not stop failover
                log.warning("provider %s raised: %s", name, exc)
                last_error = f"{name}: {exc}"
                last_provider = name
                continue
            if result is None:
                last_error = f"{name}: returned no result"
                last_provider = name
                continue
            if result.ok:
                if result.provider is None:
                    result.provider = name
                return result
            log.warning("provider %s failed: %s", name, result.error)
            last_error = result.error or f"{name}: unknown error"
            last_provider = name
        return AIResult(ok=False, error=last_error, provider=last_provider)
      

def resolve_verdict(prior: AIResult, verdict: Optional[AIResult], *, enabled: bool) -> AIResult:
    """Decide the post-verification result. Pure: no I/O, no service state.

    Policy, in strict branch order:
      1. verification disabled            -> prior unchanged
      2. verdict missing / not ok / no data (verifier unreachable)
                                          -> FAIL OPEN: prior unchanged
      3. supported is True                -> prior unchanged
      4. supported False + corrected_body -> prior with body replaced (stays ok)
      5. supported False + no correction  -> FAILED result (caller holds the post)
    """
    if not enabled:
        return prior
    if verdict is None or not verdict.ok or not verdict.data:
        return prior  # fail open: verifier unavailable must not block the pipeline
    if verdict.data.get("supported") is True:
        return prior
    corrected = verdict.data.get("corrected_body")
    if corrected:
        fixed = dict(prior.data or {})
        fixed["body"] = corrected
        return AIResult(ok=True, data=fixed, provider=prior.provider)
    return AIResult(ok=False, error="verification: unsupported claims", provider=prior.provider)
