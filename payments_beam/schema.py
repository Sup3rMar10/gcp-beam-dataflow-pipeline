"""Validate one raw payment event. Pure Python, no Beam, so every rule is a fast unit test.

An event is JSON: {"event_id", "merchant", "amount_cents", "currency", "event_time"}. A bad event is not an
exception that kills the pipeline: it is a dead-letter record with the reason, kept for inspection and replay.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone

CURRENCIES = {"USD", "EUR", "GBP"}
MAX_AMOUNT_CENTS = 10_000_000
MAX_RAW_CHARS = 10_000
_ID = re.compile(r"[A-Za-z0-9_-]{1,64}")


class InvalidEvent(Exception):
    """The reason an event was rejected, written for a human reading the dead-letter table."""


def parse_event(raw) -> dict:
    """Return the validated event with an added `ts` (event time, epoch seconds), or raise InvalidEvent."""
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError:
            raise InvalidEvent("not valid UTF-8") from None
    if not isinstance(raw, str):
        raise InvalidEvent(f"expected text, got {type(raw).__name__}")
    if len(raw) > MAX_RAW_CHARS:
        raise InvalidEvent(f"message is {len(raw)} characters, over the {MAX_RAW_CHARS} limit")
    try:
        event = json.loads(raw)
    except ValueError:
        raise InvalidEvent("not valid JSON") from None
    if not isinstance(event, dict):
        raise InvalidEvent("the JSON is not an object")

    for field in ("event_id", "merchant"):
        value = event.get(field)
        if not isinstance(value, str) or not _ID.fullmatch(value):
            raise InvalidEvent(f"{field} must be 1-64 characters of letters, digits, '_' or '-'")
    amount = event.get("amount_cents")
    if isinstance(amount, bool) or not isinstance(amount, int) or not 1 <= amount <= MAX_AMOUNT_CENTS:
        raise InvalidEvent(f"amount_cents must be a whole number from 1 to {MAX_AMOUNT_CENTS}")
    if event.get("currency") not in CURRENCIES:
        raise InvalidEvent(f"currency must be one of {sorted(CURRENCIES)}")
    et = event.get("event_time")
    if not isinstance(et, str):
        raise InvalidEvent("event_time must be an ISO 8601 string")
    try:
        parsed = datetime.fromisoformat(et.replace("Z", "+00:00"))
    except ValueError:
        raise InvalidEvent("event_time is not a valid ISO 8601 timestamp") from None
    if parsed.tzinfo is None:
        raise InvalidEvent("event_time must include a timezone (use Z for UTC)")
    return {**event, "ts": parsed.astimezone(timezone.utc).timestamp()}


def dead_letter(raw, reason: str) -> dict:
    """The record written for a rejected message: what it said (truncated) and why it was rejected."""
    text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else str(raw)
    return {"raw": text[:1000], "reason": reason, "truncated": len(text) > 1000}
