"""python -m payments_beam batch [--input data/events.jsonl] [--output out]"""

from __future__ import annotations

import argparse
import logging

from .pipeline import run_batch


def main() -> int:
    logging.getLogger().setLevel(logging.ERROR)
    p = argparse.ArgumentParser(prog="payments_beam")
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("batch", help="run the transform over a JSONL file with the DirectRunner")
    b.add_argument("--input", default="data/events.jsonl")
    b.add_argument("--output", default="out")
    a = p.parse_args()

    result = run_batch(a.input, a.output)
    print(f"{'merchant':<8} {'window':<22} {'total':>7} {'count':>5} {'dupes':>5} {'pane':<8}")
    for r in sorted(result["totals"], key=lambda r: (r["window_start"], r["merchant"])):
        print(f"{r['merchant']:<8} {r['window_start'][11:19]}-{r['window_end'][11:19]:<12} {r['total_cents']:>7} "
              f"{r['count']:>5} {r['duplicates']:>5} {r['pane']:<8}")
    print(f"\n{len(result['dead'])} message(s) went to the dead-letter output:")
    for d in result["dead"]:
        print(f"  {d['reason']}: {d['raw'][:50]!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
