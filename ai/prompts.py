"""Prompt templates for generation and verification.

A Prompt bundles the system instruction and the user message. Builder
functions return fully-formed Prompt objects so callers never assemble
prompt strings inline.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Prompt:
    system: str
    user: str


GENERATE_SYSTEM = (
    "You are a content writer. Produce a single JSON object with exactly two "
    'keys: "title" and "body". The title is a short headline. The body is the '
    "full post text. Do not include any text outside the JSON object."
)

VERIFY_SYSTEM = (
    "You are a fact-checking editor. You are given a draft post and its source "
    "material. Decide whether every claim in the draft is supported by the "
    "source. Respond with a single JSON object with these keys: "
    '"supported" (true or false), and "corrected_body" (a rewritten body that '
    "removes or fixes unsupported claims, or null if no correction is needed). "
    "Do not include any text outside the JSON object."
)


def build_generate_prompt(topic: str, source_material: str) -> Prompt:
    """Prompt that asks a provider to draft a post from a topic and sources."""
    user = (
        f"Topic:\n{topic}\n\n"
        f"Source material:\n{source_material}\n\n"
        "Write the post now as the required JSON object."
    )
    return Prompt(system=GENERATE_SYSTEM, user=user)


def build_verify_prompt(draft_body: str, source_material: str) -> Prompt:
    """Prompt that asks a provider to fact-check a draft against its sources."""
    user = (
        f"Draft post:\n{draft_body}\n\n"
        f"Source material:\n{source_material}\n\n"
        "Check the draft against the source material and respond with the "
        "required JSON object."
    )
    return Prompt(system=VERIFY_SYSTEM, user=user)
