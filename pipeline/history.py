"""
The Commons Index — history runner.

Reads a chosen slice of past Hansard (from -> to), classifies every
contribution with the exact same locked prompt and menus as the nightly
run, and files the verdicts into the Supabase `speech` table.

DELIBERATELY TOUCHES NOTHING ELSE:
  - never writes to data/*.json (the live site is untouched)
  - never changes state.json (the nightly's bookmark is untouched)
  - safe to run any weekend, any range; the pointed end of the backfill

Usage:  python pipeline/history.py --frm 2026-06-22 --to 2026-06-26
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run as engine  # noqa: E402  (the nightly pipeline, used as a library)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--frm", required=True, help="start date YYYY-MM-DD")
    ap.add_argument("--to", required=True, help="end date YYYY-MM-DD")
    args = ap.parse_args()
    frm, to = args.frm.strip(), args.to.strip()
    if not (len(frm) == 10 and len(to) == 10 and frm <= to):
        sys.exit(f"Bad range: {frm} -> {to}")

    print(f"=== History run · {frm} -> {to} ===")
    if not os.environ.get("SUPABASE_URL") or \
       not os.environ.get("SUPABASE_SERVICE_KEY"):
        sys.exit("No Supabase credentials in environment — nothing to "
                 "file into, refusing to spend on classification.")

    # history material can be any age: never age it out mid-run
    engine.CLASS_PENDING_MAX_DAYS = 10_000

    members = engine.fetch_members()

    # walk ONLY the requested window; throwaway state/debates/mp_q so the
    # nightly's own files and bookmarks are never touched
    state = {"last_debate_date": frm}
    debates, mp_q, pending = {}, {}, []
    engine.fetch_debates(state, debates, mp_q, pending, until=to)
    pending[:] = [d for d in pending if frm <= d.get("d", "") <= to]
    n_items = sum(len(d["items"]) for d in pending)
    print(f"History: {len(pending)} debates, {n_items} contributions "
          f"in window")
    if not pending:
        print("History: nothing in range (recess?) — done")
        return

    # classify until the window is fully marked (the per-call chunk cap
    # still applies inside each pass, as a cost guard)
    passes = 0
    while pending and passes < 40:
        passes += 1
        before = sum(1 for d in pending for i in d["items"]
                     if not i.get("done"))
        engine.ai_classify_debates(pending, mp_q, members)
        after = sum(1 for d in pending for i in d["items"]
                    if not i.get("done"))
        if after >= before:  # no progress (e.g. API down) — stop cleanly
            print("History: no progress this pass — stopping; "
                  "re-run later to finish the remainder")
            break

    engine.push_to_db(mp_q)
    print("=== History run complete ===")


if __name__ == "__main__":
    main()
