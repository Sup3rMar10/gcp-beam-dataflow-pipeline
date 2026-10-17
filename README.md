# gcp-beam-dataflow-pipeline

![ci](https://github.com/Sup3rMar10/gcp-beam-dataflow-pipeline/actions/workflows/ci.yml/badge.svg)

A **Dataflow-style streaming pipeline** in Apache Beam, built so its hard parts are testable without Google Cloud: bad messages go to a **dead-letter output** instead of failing the job, **duplicates** are not double counted, and per-merchant totals are computed over **event-time windows** with a defined policy for **late data**, tested deterministically with `TestStream`, where the test controls the watermark.

> **What is verified, and what is not.** 95 tests pass locally on Python 3.13 with Apache Beam 2.77, and GitHub Actions on Linux passed the same tests on Python 3.11 and 3.13, the batch run over the sample file, and `actionlint` (2026-10-17). The pipeline really runs (the Beam local runner executes it in the tests), including streaming scenarios with an exact watermark. **Nothing has run on Dataflow**: there is no project, Pub/Sub topic or BigQuery table. The Pub/Sub-to-BigQuery job (`run_stream`) and the Flex Template packaging were never launched or built; the tests only check that their arguments, schemas and files are consistent with each other.

## What it does (`payments_beam/`)

```
Pub/Sub  (event time = the message's timestamp attribute)
   |
 Parse + validate --reject--> dead-letter output (raw text, reason)
   |
 Window: fixed 60s event-time windows, 120s allowed lateness
   |
 Per merchant: unique-event total, count, duplicates, conflicts
   |
 BigQuery rows (merchant, window_start, window_end, total_cents, count, duplicates, conflicts, pane)
```

| Concern | Behaviour | Tested |
|---|---|---|
| Bad input | Never fails the job. 26 invalid shapes (bad JSON, non-object, bad ids, amounts that are 0, negative, huge, fractional, a string or a bool, a currency outside USD/EUR/GBP, a missing or timezone-less `event_time`, oversized or non-UTF-8 text) each become a dead-letter record naming the reason | yes |
| Wrong window | If the payload's `event_time` disagrees with the element's timestamp the event would be counted in the wrong window unnoticed, so it is dead-lettered with that reason | yes |
| Duplicates | The combiner remembers each event id, so a repeated event is counted once, in any order or grouping Beam chooses (40 randomised regroupings give identical results) | yes |
| Conflicts | The same id with a different amount is **reported** (`conflicts`) and resolved to the smallest, never silently | yes |
| On time | One `ON_TIME` pane per merchant and window when the watermark passes the window end | yes |
| Late, within 120s | A corrected row with the **full** new total (`LATE` pane, accumulating), not just the late event | yes |
| Later than 120s | Dropped (the window is closed for good) | yes |
| Late duplicate | Updates the `duplicates` count, **not** the total (at-least-once delivery cannot double count) | yes |

**Consumers must upsert** on `(merchant, window_start)` and keep the latest pane. Summing the rows would double count by design.

## Things I got wrong, and fixed

- **A batch run emitted every window twice** (an identical `ON_TIME` and `LATE` pane) when I used the late-data triggers for files too. A file has no "late". The triggers are now streaming-only, and a test asserts each window appears exactly once in batch.
- **A separate dedupe step would have double counted.** A `GroupByKey` before the totals re-emits its panes under accumulating mode. Deduplication therefore lives inside the combiner.
- **My test epochs were off by a year** (I typed October 2025's epoch). The tests now derive every time from `datetime`.
- Re-stamping an element with an *earlier* timestamp than it arrived with is rejected by Beam, which is why the timestamp comes from the source (`timestamp_attribute`) in streaming and from `WithEventTimestamps` only for files.

## Honest limits

- **The local runner is not Dataflow.** Watermarks in `TestStream` are exactly what the test says; a real Pub/Sub source *estimates* its watermark, so which events count as late in production depends on publish delays and backlog. Allowed lateness needs tuning against real data.
- **Deduplication is per key and window.** A duplicate that lands in a different window is not caught, and the accumulator holds one entry per unique event id per merchant per window (memory grows with traffic). A real system would also rely on Pub/Sub exactly-once delivery or a durable id store.
- **Late events beyond the allowed lateness are dropped silently** (Beam does not hand them to you). If that matters, route a second, delayed copy of the stream to a reconciliation job.
- BigQuery streaming inserts can themselves deliver duplicates; the upsert-on-key rule for consumers covers that, but nothing here enforces it.
- Money is integer cents in three currencies; there is no currency conversion and totals are per currency only if merchants use one currency (a merchant that sends several would have them summed together: a real gap).
- `run_stream`, `flex/Dockerfile` and `deploy.yml` are reference code, never run. The Flex Template image was never built.

## Run it

```
python -m pip install apache-beam pytest pyyaml
python -m pytest                         # about 6 seconds once Beam's local runner is cached
python -m payments_beam batch            # the sample file: 4 windows, 3 dead letters
```

`data/events.jsonl` includes a duplicate, a negative amount, an unsupported currency and a line that is not JSON.
