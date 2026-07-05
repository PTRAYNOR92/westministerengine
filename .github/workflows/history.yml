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


def _next_range(cursor, stop):
    """The fortnight ending the day before `cursor`, clamped at `stop`."""
    from datetime import date, timedelta
    c = date.fromisoformat(cursor)
    to = c - timedelta(days=1)
    frm = max(to - timedelta(days=13), date.fromisoformat(stop))
    return frm.isoformat(), to.isoformat()


def _oldest_in_db():
    """Ask the cabinet for its oldest stored date (autopilot's default
    starting cursor)."""
    import requests as rq
    url = os.environ["SUPABASE_URL"].rstrip("/")
    key = os.environ["SUPABASE_SERVICE_KEY"]
    r = rq.get(f"{url}/rest/v1/speech",
               params={"select": "said_on", "order": "said_on.asc",
                       "limit": 1},
               headers={"apikey": key, "authorization": f"Bearer {key}"},
               timeout=60)
    r.raise_for_status()
    rows = r.json()
    if not rows:
        sys.exit("Autopilot: the database is empty — run one manual "
                 "range first so there is an edge to walk back from.")
    return rows[0]["said_on"]


def _signal(cont, cursor):
    """Tell the workflow whether to dispatch the next domino."""
    out = os.environ.get("GITHUB_OUTPUT")
    line = f"continue={'true' if cont else 'false'}\ncursor={cursor}\n"
    if out:
        with open(out, "a") as f:
            f.write(line)
    print(f"Autopilot signal: {line.strip()}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--frm", help="start date YYYY-MM-DD (manual mode)")
    ap.add_argument("--to", help="end date YYYY-MM-DD (manual mode)")
    ap.add_argument("--auto", action="store_true",
                    help="autopilot: work out the next fortnight itself")
    ap.add_argument("--cursor", default="",
                    help="autopilot: walk back from this date "
                         "(default: oldest date already in the database)")
    ap.add_argument("--stop", default="2024-07-04",
                    help="autopilot: do not go earlier than this date")
    args = ap.parse_args()

    if not os.environ.get("SUPABASE_URL") or \
       not os.environ.get("SUPABASE_SERVICE_KEY"):
        sys.exit("No Supabase credentials in environment — nothing to "
                 "file into, refusing to spend on classification.")

    if args.auto:
        cursor = args.cursor.strip() or _oldest_in_db()
        if cursor <= args.stop:
            print(f"Autopilot: reached the stop date ({args.stop}) — "
                  f"the backfill is finished.")
            _signal(False, cursor)
            return
        frm, to = _next_range(cursor, args.stop)
    else:
        frm, to = (args.frm or "").strip(), (args.to or "").strip()
        if not (len(frm) == 10 and len(to) == 10 and frm <= to):
            sys.exit(f"Bad range: {frm} -> {to}")

    print(f"=== History run · {frm} -> {to} ===")

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
        if args.auto:
            _signal(True, frm)  # keep walking back past the recess
        return

    # classify until the window is fully marked (the per-call chunk cap
    # still applies inside each pass, as a cost guard). If a pass makes
    # no progress (e.g. the AI line is busy), wait and retry once more
    # before giving up — and if anything is left unclassified, exit
    # loudly so the run shows RED, never a false green.
    import time
    passes, stalls = 0, 0
    while pending and passes < 40:
        passes += 1
        before = sum(1 for d in pending for i in d["items"]
                     if not i.get("done"))
        engine.ai_classify_debates(pending, mp_q, members)
        after = sum(1 for d in pending for i in d["items"]
                    if not i.get("done"))
        if after >= before:
            stalls += 1
            if stalls >= 2:
                print("History: no progress after retry — stopping")
                break
            print("History: no progress this pass — waiting 90s for the "
                  "AI line to clear, then retrying…")
            time.sleep(90)
        else:
            stalls = 0

    engine.push_to_db(mp_q)
    left = sum(1 for d in pending for i in d["items"] if not i.get("done"))
    if left:
        print(f"=== History run INCOMPLETE: {left} contributions "
              f"unclassified — re-run this same range to finish ===")
        sys.exit(1)
    if args.auto:
        _signal(True, frm)  # done: next domino starts the day before frm
    print("=== History run complete ===")


if __name__ == "__main__":
    main()
