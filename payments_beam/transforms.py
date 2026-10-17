"""The pipeline's transforms: parse and validate, deduplicate and total per merchant per event-time window.

Design choices worth knowing:
  - Bad input never fails the pipeline: it goes to a `dead` output with the reason.
  - Deduplication lives INSIDE the combiner (it remembers the amounts it has seen per event id). The alternative, a
    separate GroupByKey, would re-emit its panes into the totals under accumulating mode and double count; this way
    every pane is simply the full, correct state of the window so far.
  - Panes are ACCUMULATING: a late event produces a corrected row with the new full total. Consumers must upsert on
    (merchant, window_start) and keep the latest pane; they must not add rows together.
"""

from __future__ import annotations

from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.transforms import trigger
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.utils.windowed_value import PaneInfoTiming

from .schema import InvalidEvent, dead_letter, parse_event

VALID, DEAD = "valid", "dead"
TIMESTAMP_TOLERANCE_SECONDS = 1.0


class WithEventTimestamps(beam.DoFn):
    """For batch input (files): stamp each valid event with the time in its own payload.

    In streaming, the source assigns the timestamp (Pub/Sub's `timestamp_attribute`), so this is not used. Messages
    that cannot be parsed keep their default timestamp and are dead-lettered by ParseEvents.
    """

    def process(self, raw):
        try:
            ev = parse_event(raw)
        except InvalidEvent:
            yield raw
            return
        yield TimestampedValue(raw, ev["ts"])


class ParseEvents(beam.DoFn):
    """Validate each message. Valid events go to the main output, rejects to `dead` with a reason."""

    def __init__(self):
        self.valid = Metrics.counter("payments", "valid_events")
        self.invalid = Metrics.counter("payments", "invalid_events")

    def process(self, raw, timestamp=beam.DoFn.TimestampParam):
        try:
            ev = parse_event(raw)
        except InvalidEvent as e:
            self.invalid.inc()
            yield beam.pvalue.TaggedOutput(DEAD, dead_letter(raw, str(e)))
            return
        # The window an event lands in comes from the element timestamp, so a payload that disagrees with it would
        # be counted in the wrong window without anyone noticing. Reject it visibly instead.
        if abs(ev["ts"] - float(timestamp)) > TIMESTAMP_TOLERANCE_SECONDS:
            self.invalid.inc()
            yield beam.pvalue.TaggedOutput(DEAD, dead_letter(raw, "event_time does not match the element timestamp"))
            return
        self.valid.inc()
        yield ev


class TotalsFn(beam.CombineFn):
    """Per merchant and window: unique-event total and count, plus how many duplicates and conflicts were seen.

    The accumulator remembers every amount seen for each event id, so merging is order-independent (a requirement
    for a CombineFn, which Beam may apply in any grouping). A duplicate is the same id again; a conflict is the same
    id with a different amount, resolved deterministically to the smallest and reported, never silently.
    """

    def create_accumulator(self):
        return {"seen": {}, "inputs": 0}

    def add_input(self, acc, ev):
        acc["seen"].setdefault(ev["event_id"], set()).add(ev["amount_cents"])
        acc["inputs"] += 1
        return acc

    def merge_accumulators(self, accs):
        merged = self.create_accumulator()
        for a in accs:
            merged["inputs"] += a["inputs"]
            for event_id, amounts in a["seen"].items():
                merged["seen"].setdefault(event_id, set()).update(amounts)
        return merged

    def extract_output(self, acc):
        seen = acc["seen"]
        return {
            "total_cents": sum(min(amounts) for amounts in seen.values()),
            "count": len(seen),
            "duplicates": acc["inputs"] - len(seen),
            "conflicts": sum(1 for amounts in seen.values() if len(amounts) > 1),
        }


def _iso(ts) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


class ToRow(beam.DoFn):
    def process(self, kv, window=beam.DoFn.WindowParam, pane=beam.DoFn.PaneInfoParam):
        merchant, totals = kv
        yield {"merchant": merchant, "window_start": _iso(window.start), "window_end": _iso(window.end), **totals,
               "pane": PaneInfoTiming.to_string(pane.timing)}


class PaymentTotals(beam.PTransform):
    """raw messages (already timestamped) -> {"totals": rows, "dead": rejects}."""

    def __init__(self, window_seconds: int = 60, allowed_lateness_seconds: int = 120, streaming: bool = False):
        super().__init__()
        self.window_seconds = window_seconds
        self.allowed_lateness_seconds = allowed_lateness_seconds
        self.streaming = streaming

    def _window(self):
        if not self.streaming:
            # A file has no "late": every window fires once, when the input is exhausted. (Late-data triggers on a
            # batch run printed each window twice, an identical ON_TIME and LATE pane, so they are streaming-only.)
            return beam.WindowInto(FixedWindows(self.window_seconds))
        return beam.WindowInto(
            FixedWindows(self.window_seconds),
            trigger=trigger.AfterWatermark(late=trigger.AfterCount(1)),  # one on-time pane, then one per late event
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=self.allowed_lateness_seconds)

    def expand(self, raw):
        parsed = raw | "Parse" >> beam.ParDo(ParseEvents()).with_outputs(DEAD, main=VALID)
        totals = (
            parsed[VALID]
            | "Window" >> self._window()
            | "KeyByMerchant" >> beam.Map(lambda e: (e["merchant"], e))
            | "Total" >> beam.CombinePerKey(TotalsFn())
            | "ToRow" >> beam.ParDo(ToRow())
        )
        return {"totals": totals, "dead": parsed[DEAD]}
