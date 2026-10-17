"""Beam tests: a real batch run, and deterministic streaming scenarios with TestStream (watermarks under our control)."""

import json
from datetime import datetime, timezone
from pathlib import Path

import apache_beam as beam
import pytest
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue

from payments_beam import PaymentTotals
from payments_beam.pipeline import run_batch

ROOT = Path(__file__).resolve().parent.parent
BASE = int(datetime(2026, 10, 17, 10, 0, 0, tzinfo=timezone.utc).timestamp())  # window 0 is [BASE, BASE+60)


def iso(offset):
    return datetime.fromtimestamp(BASE + offset, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def ev(event_id, merchant, cents, offset, currency="USD", stamp_offset=None):
    """A stream element: the JSON payload, timestamped (as a Pub/Sub timestamp_attribute would) with its event time."""
    body = {"event_id": event_id, "merchant": merchant, "amount_cents": cents, "currency": currency, "event_time": iso(offset)}
    return TimestampedValue(json.dumps(body), BASE + (offset if stamp_offset is None else stamp_offset))


def streaming_options():
    opts = PipelineOptions()
    opts.view_as(StandardOptions).streaming = True
    return opts


def summarise(r):
    start = int(datetime.fromisoformat(r["window_start"].replace("Z", "+00:00")).timestamp()) - BASE
    return (r["merchant"], start, r["total_cents"], r["count"], r["pane"])


def run_stream(stream):
    """Run `stream` through the transform; the rows are printed as TOTAL/DEAD lines for the capture fixture to read."""
    with beam.Pipeline(options=streaming_options()) as p:
        out = p | stream | PaymentTotals(60, 120, streaming=True)
        out["totals"] | "T" >> beam.Map(lambda r: print("TOTAL", json.dumps(r)))
        out["dead"] | "D" >> beam.Map(lambda r: print("DEAD", json.dumps(r)))


@pytest.fixture()
def capture(capfd):
    def go(stream):
        run_stream(stream)
        out = capfd.readouterr().out
        t = [summarise(json.loads(line[6:])) for line in out.splitlines() if line.startswith("TOTAL ")]
        full = [json.loads(line[6:]) for line in out.splitlines() if line.startswith("TOTAL ")]
        d = [json.loads(line[5:])["reason"] for line in out.splitlines() if line.startswith("DEAD ")]
        return t, d, full

    return go


# --- streaming: watermarks, lateness and corrections ---------------------------------------------------------


def test_on_time_events_produce_one_on_time_pane_per_merchant_and_window(capture):
    stream = (TestStream().advance_watermark_to(BASE)
              .add_elements([ev("a", "m1", 100, 5), ev("b", "m1", 50, 30), ev("c", "m2", 70, 20), ev("d", "m1", 5, 65)])
              .advance_watermark_to(BASE + 130).advance_watermark_to_infinity())
    totals, dead, _ = capture(stream)
    assert sorted(totals) == sorted([("m1", 0, 150, 2, "ON_TIME"), ("m2", 0, 70, 1, "ON_TIME"), ("m1", 60, 5, 1, "ON_TIME")])
    assert dead == []


def test_a_late_event_inside_the_allowed_lateness_produces_a_corrected_total(capture):
    stream = (TestStream().advance_watermark_to(BASE)
              .add_elements([ev("a", "m1", 100, 5), ev("b", "m1", 50, 30)])
              .advance_watermark_to(BASE + 61)  # the window closes: the ON_TIME pane fires
              .add_elements([ev("c", "m1", 25, 40)])  # arrives after the window closed, but is within 120s
              .advance_watermark_to_infinity())
    totals, _, _ = capture(stream)
    assert sorted(totals) == sorted([("m1", 0, 150, 2, "ON_TIME"), ("m1", 0, 175, 3, "LATE")]), \
        "the LATE pane carries the FULL corrected total (accumulating), not just the late event"


def test_an_event_later_than_the_allowed_lateness_is_dropped(capture):
    stream = (TestStream().advance_watermark_to(BASE)
              .add_elements([ev("a", "m1", 100, 5)])
              .advance_watermark_to(BASE + 61)
              .advance_watermark_to(BASE + 200)  # past window end + 120s of lateness: the window is closed for good
              .add_elements([ev("z", "m1", 999, 10)])
              .advance_watermark_to_infinity())
    totals, _, _ = capture(stream)
    assert totals == [("m1", 0, 100, 1, "ON_TIME")], "the 999 never appears anywhere"


def test_a_late_duplicate_corrects_the_duplicate_count_but_not_the_total(capture):
    stream = (TestStream().advance_watermark_to(BASE)
              .add_elements([ev("a", "m1", 100, 5)])
              .advance_watermark_to(BASE + 61)
              .add_elements([ev("a", "m1", 100, 5)])  # the same event delivered again, late
              .advance_watermark_to_infinity())
    totals, _, full = capture(stream)
    assert sorted(totals) == sorted([("m1", 0, 100, 1, "ON_TIME"), ("m1", 0, 100, 1, "LATE")])
    late = next(r for r in full if r["pane"] == "LATE")
    assert late["duplicates"] == 1 and late["total_cents"] == 100, "at-least-once delivery must not double count"


def test_duplicates_within_the_window_are_counted_once(capture):
    stream = (TestStream().advance_watermark_to(BASE)
              .add_elements([ev("a", "m1", 100, 5), ev("a", "m1", 100, 5), ev("a", "m1", 100, 5), ev("b", "m1", 1, 7)])
              .advance_watermark_to_infinity())
    totals, _, full = capture(stream)
    assert totals == [("m1", 0, 101, 2, "ON_TIME")] and full[0]["duplicates"] == 2


def test_the_same_id_with_a_different_amount_is_reported_as_a_conflict(capture):
    stream = (TestStream().advance_watermark_to(BASE)
              .add_elements([ev("a", "m1", 100, 5), ev("a", "m1", 70, 6)])
              .advance_watermark_to_infinity())
    _, _, full = capture(stream)
    assert full[0]["total_cents"] == 70 and full[0]["conflicts"] == 1


def test_invalid_and_inconsistent_messages_go_to_the_dead_letter_output_not_into_totals(capture):
    stream = (TestStream().advance_watermark_to(BASE)
              .add_elements([
                  ev("a", "m1", 100, 5),
                  ev("bad", "m1", -1, 6),
                  ev("jpy", "m1", 100, 7, currency="JPY"),
                  TimestampedValue("this is not json", BASE + 8),
                  ev("skew", "m1", 500, 10, stamp_offset=70),  # payload says 10s, the element is stamped 70s: would land in the wrong window
              ])
              .advance_watermark_to_infinity())
    totals, dead, _ = capture(stream)
    assert totals == [("m1", 0, 100, 1, "ON_TIME")]
    assert len(dead) == 4
    assert any("amount_cents" in d for d in dead) and any("currency" in d for d in dead)
    assert any("not valid JSON" in d for d in dead) and any("does not match the element timestamp" in d for d in dead)


# --- batch: the same transform over a file --------------------------------------------------------------------


def test_the_batch_run_over_the_sample_file_gives_the_expected_totals_and_dead_letters(tmp_path):
    result = run_batch(str(ROOT / "data" / "events.jsonl"), str(tmp_path))
    got = sorted((r["merchant"], r["window_start"][11:19], r["total_cents"], r["count"], r["duplicates"], r["pane"]) for r in result["totals"])
    assert got == sorted([
        ("acme", "10:00:00", 2050, 2, 1, "ON_TIME"), ("globex", "10:00:00", 5000, 1, 0, "ON_TIME"),
        ("acme", "10:01:00", 300, 1, 0, "ON_TIME"), ("globex", "10:01:00", 700, 1, 0, "ON_TIME")])
    assert len(result["dead"]) == 3
    assert sorted(d["reason"].split(" ")[0] for d in result["dead"]) == ["amount_cents", "currency", "not"]


def test_a_batch_run_emits_each_window_exactly_once(tmp_path):
    """Late-data triggers on a batch run printed every window twice; this guards the fix."""
    result = run_batch(str(ROOT / "data" / "events.jsonl"), str(tmp_path))
    keys = [(r["merchant"], r["window_start"]) for r in result["totals"]]
    assert len(keys) == len(set(keys)) == 4


def test_the_output_files_are_written_as_json_lines(tmp_path):
    run_batch(str(ROOT / "data" / "events.jsonl"), str(tmp_path))
    for name in ("totals.jsonl", "dead_letter.jsonl"):
        lines = (tmp_path / name).read_text(encoding="utf-8").splitlines()
        assert lines and all(isinstance(json.loads(line), dict) for line in lines)


def test_a_file_of_only_garbage_produces_only_dead_letters(tmp_path):
    f = tmp_path / "bad.jsonl"
    f.write_text("nope\n{}\n[1]\n", encoding="utf-8")
    result = run_batch(str(f), str(tmp_path / "out"))
    assert result["totals"] == [] and len(result["dead"]) == 3
