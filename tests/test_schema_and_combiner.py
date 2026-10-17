import json
import random

import pytest

from payments_beam import InvalidEvent, TotalsFn, dead_letter, parse_event

GOOD = {"event_id": "e1", "merchant": "acme", "amount_cents": 1250, "currency": "USD", "event_time": "2026-10-17T10:00:05Z"}


def raw(**over):
    d = {**GOOD, **over}
    for k in [k for k, v in d.items() if v is ...]:
        del d[k]
    return json.dumps(d)


def reason(text):
    with pytest.raises(InvalidEvent) as e:
        parse_event(text)
    return str(e.value)


# --- validation ---------------------------------------------------------------------------------------------


def test_a_good_event_parses_and_gets_an_epoch_timestamp():
    ev = parse_event(raw())
    assert ev["merchant"] == "acme" and ev["ts"] == 1_792_231_205.0


def test_bytes_are_accepted_as_pubsub_delivers_them():
    assert parse_event(raw().encode())["event_id"] == "e1"


def test_time_zones_are_normalised_to_utc():
    assert parse_event(raw(event_time="2026-10-17T12:00:05+02:00"))["ts"] == 1_792_231_205.0


@pytest.mark.parametrize(
    "text,fragment",
    [
        ("", "not valid JSON"),
        ("not json", "not valid JSON"),
        ("[1, 2]", "not an object"),
        ("null", "not an object"),
        (raw(event_id=...), "event_id"),
        (raw(event_id=""), "event_id"),
        (raw(event_id="a b"), "event_id"),
        (raw(event_id="x\n"), "event_id"),
        (raw(event_id="a" * 65), "event_id"),
        (raw(event_id=5), "event_id"),
        (raw(merchant="../etc"), "merchant"),
        (raw(merchant=...), "merchant"),
        (raw(amount_cents=0), "amount_cents"),
        (raw(amount_cents=-5), "amount_cents"),
        (raw(amount_cents=10_000_001), "amount_cents"),
        (raw(amount_cents=12.5), "amount_cents"),
        (raw(amount_cents="12"), "amount_cents"),
        (raw(amount_cents=True), "amount_cents"),
        (raw(currency="JPY"), "currency"),
        (raw(currency="usd"), "currency"),
        (raw(currency=...), "currency"),
        (raw(event_time=...), "event_time must be an ISO"),
        (raw(event_time=1760695205), "event_time must be an ISO"),
        (raw(event_time="yesterday"), "valid ISO 8601"),
        (raw(event_time="2026-10-17T10:00:05"), "timezone"),
        ("x" * 10_001, "over the 10000 limit"),
    ],
)
def test_bad_events_are_rejected_with_a_reason_naming_the_problem(text, fragment):
    assert fragment in reason(text)


def test_non_text_input_and_invalid_utf8_are_rejected_not_crashed():
    assert "not valid UTF-8" in reason(b"\xff\xfe")
    assert "expected text" in reason(12345)


def test_the_dead_letter_record_keeps_what_was_said_and_truncates_a_huge_message():
    d = dead_letter("y" * 5000, "because")
    assert d["reason"] == "because" and len(d["raw"]) == 1000 and d["truncated"] is True
    assert dead_letter(b"\xffabc", "x")["raw"].endswith("abc")
    assert dead_letter("short", "x")["truncated"] is False


# --- the combiner ---------------------------------------------------------------------------------------------


def ev(i, cents):
    return {"event_id": i, "amount_cents": cents}


def combine(events, splits=1, seed=0):
    """Combine events the way Beam may: split across accumulators, then merge in some order."""
    fn = TotalsFn()
    rng = random.Random(seed)
    events = list(events)
    rng.shuffle(events)
    groups = [events[i::splits] for i in range(splits)]
    accs = []
    for g in groups:
        acc = fn.create_accumulator()
        for e in g:
            acc = fn.add_input(acc, e)
        accs.append(acc)
    rng.shuffle(accs)
    return fn.extract_output(fn.merge_accumulators(accs))


def test_totals_count_unique_events_and_report_duplicates():
    assert combine([ev("a", 100), ev("b", 50), ev("a", 100)]) == {"total_cents": 150, "count": 2, "duplicates": 1, "conflicts": 0}


def test_the_same_id_with_different_amounts_is_a_reported_conflict_resolved_to_the_smallest():
    out = combine([ev("a", 100), ev("a", 70), ev("a", 90)])
    assert out == {"total_cents": 70, "count": 1, "duplicates": 2, "conflicts": 1}


def test_an_empty_window_is_zero():
    assert combine([]) == {"total_cents": 0, "count": 0, "duplicates": 0, "conflicts": 0}


@pytest.mark.parametrize("seed", range(40))
def test_the_result_does_not_depend_on_how_beam_groups_or_orders_the_inputs(seed):
    rng = random.Random(1000 + seed)
    events = [ev(f"id{rng.randint(0, 15)}", rng.choice([10, 20, 30, 40])) for _ in range(rng.randint(1, 60))]
    expected = combine(events, splits=1, seed=0)
    for splits in (2, 3, 7):
        assert combine(events, splits=splits, seed=seed) == expected
