"""
Westminster Engine — phase one data pipeline.

Pulls from the official UK Parliament APIs (all free, no keys):
  - Members API          https://members-api.parliament.uk
  - Commons Votes API    https://commonsvotes-api.parliament.uk
  - Written Questions    https://questions-statements-api.parliament.uk

Modes:
  python pipeline/run.py --mode backfill   (one-time: everything since 4 July 2024)
  python pipeline/run.py --mode nightly    (incremental: appends since last run)

Outputs (read by the site):
  data/mps.json      per-MP cards
  data/topics.json   question-topic momentum
  data/leagues.json  league tables
  data/meta.json     freshness stamp
Internal stores (the engine's memory between runs):
  data/state.json, data/votes.json, data/q_monthly.json, data/mp_q.json
"""

import argparse
import hashlib
import html
import json
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta

import requests

START_OF_PARLIAMENT = "2024-07-04"
THEME_WINDOW_DAYS = 183   # "what they're pursuing" looks at the last ~6 months
THEME_KEEP = 30           # max question texts kept per MP
THEME_BATCH_CAP = 200     # max MP summaries regenerated per night
DATA = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
UA = {"User-Agent": "westminster-engine/1.0 (open data project)"}

MEMBERS_URL = "https://members-api.parliament.uk/api/Members/Search"
BIO_URL = "https://members-api.parliament.uk/api/Members/{}/Biography"
HANSARD_URL = "https://hansard-api.parliament.uk/search/contributions/Spoken.json"
HANSARD_CAL = "https://hansard-api.parliament.uk/overview/calendar.json"
HANSARD_SECTIONS = "https://hansard-api.parliament.uk/overview/sectionsforday.json"
HANSARD_TREES = "https://hansard-api.parliament.uk/overview/sectiontrees.json"
HANSARD_DEBATE = "https://hansard-api.parliament.uk/debates/debate/{}.json"
DEBATE_WINDOW_DAYS = 190  # how far back the debate layer looks/keeps

# --- full-speech classification (issue / stance / tone / summary) ---
CLASS_STANCES = ("supporting", "pushing further", "opposing",
                 "seeking", "constituency")
CLASS_TONES = ("heated", "impassioned", "concerned", "measured", "warm")
CLASS_MAX_CHARS = 6000       # reading limit per contribution (very long speeches)
CLASS_CHUNK = 25             # contributions per API call
CLASS_CHUNK_CAP = 300        # max chunks classified per night (cost guard)
CLASS_PENDING_MAX_DAYS = 14  # unclassified material older than this degrades
                             # to a plain snippet so nothing is ever lost
CLASS_KEEP = 50              # classified extracts kept per MP (was 12 snippets)
DIV_SEARCH = "https://commonsvotes-api.parliament.uk/data/divisions.json/search"
DIV_ONE = "https://commonsvotes-api.parliament.uk/data/division/{}.json"
WQ_URL = "https://questions-statements-api.parliament.uk/api/writtenquestions/questions"


# ---------------------------------------------------------------- helpers
def get(url, params=None, tries=4):
    """GET with polite throttling and retry/backoff."""
    for attempt in range(tries):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=60)
            if r.status_code == 200:
                time.sleep(0.25)
                return r.json()
            if r.status_code in (429, 502, 503, 504):
                wait = 5 * (attempt + 1)
                print(f"  retryable {r.status_code} on {url} — waiting {wait}s")
                time.sleep(wait)
                continue
            print(f"  HTTP {r.status_code} on {url} — skipping")
            return None
        except requests.RequestException as e:
            wait = 5 * (attempt + 1)
            print(f"  network error ({e}) — waiting {wait}s")
            time.sleep(wait)
    print(f"  giving up on {url}")
    return None


def load(name, default):
    path = os.path.join(DATA, name)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default


def save(name, obj):
    path = os.path.join(DATA, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    print(f"  wrote {name} ({os.path.getsize(path)//1024} KB)")


# ---------------------------------------------------------------- members
def fetch_members():
    print("Fetching current MPs…")
    members, skip = {}, 0
    while True:
        j = get(MEMBERS_URL, {"House": "Commons", "IsCurrentMember": "true",
                              "skip": skip, "take": 20})
        if not j or not j.get("items"):
            break
        for it in j["items"]:
            v = it["value"]
            party = (v.get("latestParty") or {}).get("abbreviation") or \
                    (v.get("latestParty") or {}).get("name") or "Ind"
            members[str(v["id"])] = {
                "id": v["id"],
                "name": v.get("nameDisplayAs", "Unknown"),
                "party": party,
                "seat": ((v.get("latestHouseMembership") or {})
                         .get("membershipFrom")) or "",
            }
        skip += 20
        if skip >= j.get("totalResults", 0):
            break
    print(f"  {len(members)} current MPs")
    return members


def _current_post(posts):
    """Return the name of the post with no end date, if any."""
    for p in posts or []:
        if isinstance(p, dict) and not p.get("endDate"):
            return (p.get("name") or "").strip()
    return ""


def _rotation_slice(members, prev, mode):
    """Which MPs to refresh tonight for the slow per-MP endpoints.

    Backfill: everyone. Nightly: a rotating seventh of the House (so every
    MP refreshes at least weekly) plus anyone not seen before. These fields
    drift slowly, and skipping six-sevenths of 647 slow API calls is what
    keeps the nightly run fast.
    """
    if mode == "backfill":
        return set(members)
    bucket = date.today().toordinal() % 7
    return {mid for mid in members
            if int(mid) % 7 == bucket or mid not in prev}


def fetch_roles(members, prev, mode):
    """Current government/opposition post per MP, from the Members API.

    One call per MP in tonight's rotation slice; everyone else (and any
    failure) carries the previous run's value so a flaky night never
    blanks the site.
    """
    todo = _rotation_slice(members, prev, mode)
    print(f"Fetching roles… ({len(todo)} of {len(members)} tonight)")
    fails = 0
    for mid, m in members.items():
        if mid not in todo:
            old = prev.get(mid, {})
            m["role"], m["roleType"] = old.get("role", ""), old.get("roleType", "")
            continue
        j = get(BIO_URL.format(mid))
        if not j:
            old = prev.get(mid, {})
            m["role"], m["roleType"] = old.get("role", ""), old.get("roleType", "")
            fails += 1
            continue
        v = j.get("value") or {}
        gov = _current_post(v.get("governmentPosts"))
        opp = _current_post(v.get("oppositionPosts"))
        if gov:
            m["role"], m["roleType"] = gov, "gov"
        elif opp:
            m["role"], m["roleType"] = opp, "opp"
        else:
            m["role"], m["roleType"] = "", ""
    print(f"  roles done ({fails} fallbacks)" if fails else "  roles done")


def fetch_spoken(members, prev, mode):
    """Count of spoken Hansard contributions this parliament, per MP.

    The Hansard search API is the slowest thing we touch, so only tonight's
    rotation slice is refreshed; everyone else carries the previous value.
    """
    todo = _rotation_slice(members, prev, mode)
    print(f"Fetching spoken contributions… ({len(todo)} of {len(members)} tonight)")
    fails = 0
    for mid, m in members.items():
        if mid not in todo:
            m["spoken"] = prev.get(mid, {}).get("spoken", 0)
            continue
        j = get(HANSARD_URL, {"queryParameters.memberId": mid,
                              "queryParameters.startDate": START_OF_PARLIAMENT,
                              "queryParameters.take": 1})
        n = None
        if j:
            for key in ("TotalResultCount", "totalResultCount", "TotalResults"):
                if isinstance(j.get(key), int):
                    n = j[key]
                    break
        if n is None:
            n = prev.get(mid, {}).get("spoken", 0)
            fails += 1
        m["spoken"] = n
    print(f"  spoken done ({fails} fallbacks)" if fails else "  spoken done")


# ---------------------------------------------------------------- divisions
def fetch_divisions(state, votes):
    print("Fetching divisions…")
    known = set(state.get("division_ids", []))
    start = state.get("last_division_check", START_OF_PARLIAMENT)
    skip, found_new = 0, 0
    while True:
        j = get(DIV_SEARCH, {"queryParameters.startDate": start,
                             "queryParameters.skip": skip,
                             "queryParameters.take": 25})
        if not j:
            break
        items = j if isinstance(j, list) else j.get("items", [])
        if not items:
            break
        for d in items:
            did = d.get("DivisionId") or d.get("divisionId")
            if did is None or str(did) in known:
                continue
            detail = get(DIV_ONE.format(did))
            if not detail:
                continue
            votes[str(did)] = {
                "date": (detail.get("Date") or "")[:10],
                "title": detail.get("Title", "")[:160],
                "ayes": [m["MemberId"] for m in (detail.get("Ayes") or [])],
                "noes": [m["MemberId"] for m in (detail.get("Noes") or [])],
            }
            known.add(str(did))
            found_new += 1
        if len(items) < 25:
            break
        skip += 25
    state["division_ids"] = sorted(known)
    state["last_division_check"] = (date.today() - timedelta(days=14)).isoformat()
    print(f"  {found_new} new divisions ({len(known)} total)")
    return found_new


# ---------------------------------------------------------------- questions
def month_windows(frm_str):
    """Yield (from, to) ISO date pairs, one per calendar month, frm → today.

    The WQ API silently truncates very large queries (a 2-year window dies
    after ~500 results), so we always fetch in month-sized chunks instead.
    """
    cur = datetime.strptime(frm_str, "%Y-%m-%d").date()
    today = date.today()
    while cur <= today:
        nxt = date(cur.year + 1, 1, 1) if cur.month == 12 else \
            date(cur.year, cur.month + 1, 1)
        yield cur.isoformat(), min(nxt - timedelta(days=1), today).isoformat()
        cur = nxt


def fetch_questions(state, q_monthly, mp_q, mode):
    print("Fetching written questions…")
    frm = START_OF_PARLIAMENT if mode == "backfill" else \
        state.get("last_question_date", START_OF_PARLIAMENT)
    total = 0
    latest = frm
    sample_cutoff = (date.today() - timedelta(days=130)).isoformat()
    theme_cutoff = (date.today() - timedelta(days=THEME_WINDOW_DAYS + 7)).isoformat()
    for w_from, w_to in month_windows(frm):
        skip, win_total, reported = 0, 0, None
        while True:
            j = get(WQ_URL, {"tabledWhenFrom": w_from, "tabledWhenTo": w_to,
                             "take": 100, "skip": skip, "house": "Commons"})
            if not j:
                print(f"  WARNING: fetch died in window {w_from}→{w_to} "
                      f"at skip={skip} — window incomplete")
                break
            if reported is None:
                reported = j.get("totalResults", 0)
            results = j.get("results") or []
            if not results:
                break
            for r in results:
                v = r.get("value") or {}
                mid = v.get("askingMemberId")
                tabled = (v.get("dateTabled") or "")[:10]
                heading = html.unescape(
                    (v.get("heading") or "Unclassified")).strip()[:80]
                body = html.unescape(
                    (v.get("answeringBodyName") or "Unknown")).strip()[:60]
                if not mid or not tabled:
                    continue
                month = tabled[:7]
                qtext = html.unescape(
                    (v.get("questionText") or "")).strip()[:240]
                qm = q_monthly.setdefault(month, {})
                h = qm.setdefault(heading, {"n": 0, "members": {}, "body": body})
                h["n"] += 1
                # keep a few raw question texts for the AI gloss (recent only)
                if tabled >= sample_cutoff and qtext \
                        and len(h.setdefault("ex", [])) < 3:
                    h["ex"].append(qtext)
                h["members"][str(mid)] = h["members"].get(str(mid), 0) + 1
                mq = mp_q.setdefault(str(mid),
                                     {"total": 0, "months": {}, "bodies": {}})
                mq["total"] += 1
                mq["months"][month] = mq["months"].get(month, 0) + 1
                mq["bodies"][body] = mq["bodies"].get(body, 0) + 1
                # keep their recent question texts for the AI theme reader
                if tabled >= theme_cutoff and qtext:
                    mq.setdefault("ex", []).append(
                        {"d": tabled, "t": qtext[:200]})
                if tabled > latest:
                    latest = tabled
                win_total += 1
            skip += 100
            if skip >= (reported or 0):
                break
        flag = "" if reported is not None and win_total >= reported \
            else "  ← SHORT, check this window"
        print(f"  {w_from} → {w_to}: {win_total} questions "
              f"(API reported {reported}){flag}")
        total += win_total
    # overlap a day so nothing slips between runs (dedupe is approximate
    # at the aggregate level; a 1-day overlap on nightly runs is negligible)
    state["last_question_date"] = (
        datetime.strptime(latest, "%Y-%m-%d") - timedelta(days=1)
    ).date().isoformat() if total else state.get("last_question_date", frm)
    print(f"  {total} questions ingested")
    return total


# ---------------------------------------------------------------- ai gloss
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"


def ai_glosses(topics_out, q_monthly):
    """One plain-English line per trending subject, written by Claude.

    Single API call for all topics. Needs ANTHROPIC_API_KEY in the
    environment; without it (or on any failure) the site simply renders
    without glosses — never blocks the pipeline.
    """
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not key:
        print("AI gloss: no ANTHROPIC_API_KEY set — skipping (site still works)")
        return
    print("AI gloss: generating…")

    # gather sample question texts per heading from recent months
    samples = defaultdict(list)
    for month in sorted(q_monthly, reverse=True)[:5]:
        for heading, h in q_monthly[month].items():
            for ex in h.get("ex", []):
                if len(samples[heading]) < 4:
                    samples[heading].append(ex)

    items = []
    for t in topics_out:
        items.append({
            "topic": t["topic"],
            "department": t["body"],
            "growth_pct": t["growth"],
            "questions_this_quarter": t["cur"],
            "top_askers": [f'{a["name"]} ({a["party"]})'
                           for a in t["askers"][:3]],
            "sample_questions": samples.get(t["topic"], []),
        })

    prompt = (
        "You are writing one-line editorial glosses for a UK parliamentary "
        "data site. For each subject below — a written-question filing "
        "heading that is trending in the Commons — write ONE sentence "
        "(max 25 words) explaining in plain English what MPs are actually "
        "probing, based on the sample questions. Be specific and factual; "
        "name the issue, not the filing label. No opinions, no speculation "
        "beyond what the samples support. If samples are empty, infer "
        "cautiously from the heading and department alone and stay vague "
        "rather than guess.\n\n"
        "Respond with ONLY a JSON array, no markdown fences, of objects "
        '{"topic": "<exact topic string>", "gloss": "<sentence>"}.\n\n'
        + json.dumps(items, ensure_ascii=False)
    )

    try:
        r = requests.post(ANTHROPIC_URL, timeout=120, headers={
            "x-api-key": key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }, json={
            "model": "claude-sonnet-4-6",
            "max_tokens": 4000,
            "messages": [{"role": "user", "content": prompt}],
        })
        if r.status_code != 200:
            print(f"AI gloss: HTTP {r.status_code} — skipping ({r.text[:200]})")
            return
        text = "".join(b.get("text", "") for b in r.json().get("content", [])
                       if b.get("type") == "text")
        text = text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        glosses = {g["topic"]: g["gloss"] for g in json.loads(text)
                   if isinstance(g, dict) and g.get("topic") and g.get("gloss")}
    except Exception as e:
        print(f"AI gloss: failed ({e}) — skipping, site still works")
        return

    hit = 0
    for t in topics_out:
        if t["topic"] in glosses:
            t["gloss"] = str(glosses[t["topic"]])[:220]
            hit += 1
    print(f"AI gloss: {hit}/{len(topics_out)} topics glossed")


def ai_mp_themes(mps_out, mp_q):
    """Per-MP 'what they're pursuing': top 3 themes with a stance line each,
    plus behavioural counts of their recent written questions.

    Cached by content signature in data/mp_glosses.json — an MP is only
    re-read when their question set changes, so steady-state cost is pennies.
    Capped at THEME_BATCH_CAP regenerations per night; the rest catch up on
    following nights. No key / API failure → cached values still render.
    """
    cache = load("mp_glosses.json", {})
    cutoff = (date.today() - timedelta(days=THEME_WINDOW_DAYS)).isoformat()

    todo = []
    for m in mps_out:
        mid = str(m["id"])
        ex = [e for e in mp_q.get(mid, {}).get("ex", [])
              if isinstance(e, dict) and e.get("d", "") >= cutoff]
        ex = sorted(ex, key=lambda e: e["d"])[-THEME_KEEP:]
        dx = [e for e in mp_q.get(mid, {}).get("deb_ex", [])
              if isinstance(e, dict) and e.get("d", "") >= cutoff]
        dx = sorted(dx, key=lambda e: e["d"])[-12:]
        if len(ex) + len(dx) < 5:
            continue  # too little material to theme honestly
        sig = hashlib.md5("|".join(e["t"] for e in ex + dx)
                          .encode("utf-8")).hexdigest()[:12]
        c = cache.get(mid)
        if c and c.get("sig") == sig and c.get("themes"):
            m["themes"], m["approach"] = c["themes"], c.get("approach", {})
            m["themeN"] = c.get("n", len(ex))
        else:
            todo.append((m, ex, dx, sig))

    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not key:
        print(f"MP themes: no key — {len(todo)} pending, cached ones still shown")
        return
    if not todo:
        print("MP themes: all up to date from cache")
        return
    todo = todo[:THEME_BATCH_CAP]
    print(f"MP themes: generating for {len(todo)} MPs…")

    done = 0
    for i in range(0, len(todo), 20):
        batch = todo[i:i + 20]
        payload = [{"id": m["id"], "name": m["name"], "party": m["party"],
                    "role": m.get("role", ""),
                    "written_questions": [e["t"] for e in ex],
                    "debate_extracts": [e["t"] for e in dx]}
                   for m, ex, dx, _ in batch]
        prompt = (
            "You are analysing UK MPs' parliamentary activity for a data "
            "site. Each MP below has written_questions and debate_extracts "
            "(short excerpts of what they said on the floor of the Commons). "
            "Using ONLY this listed material:\n"
            "1. themes: their top 1-3 subjects (most prominent first). Each "
            'has "subject" (2-5 plain words), "stance" (max 14 words on what '
            "they are doing about it — e.g. 'pressing the government over "
            "delays to compensation payments', 'seeking detail on funding "
            "allocations'; describe conduct, never characterise the person), "
            'and "eg" (the 0-based index into a COMBINED list of '
            "written_questions followed by debate_extracts, of the one item "
            "that best shows the theme).\n"
            '2. approach: count EVERY listed item (questions AND extracts) '
            'into exactly one of: '
            '"pressing" (demanding action, challenging, chasing failures), '
            '"probing" (seeking information or detail), '
            '"constituency" (local casework framing), '
            '"supportive" (inviting good news, friendly prompts, defending '
            "the government's record).\n"
            "Ground everything in the texts. Debate extracts are fragments — "
            "judge them cautiously. Respond with ONLY a "
            "JSON array, no markdown fences, of objects "
            '{"id": <id>, "themes": [...], "approach": {...}}.\n\n'
            + json.dumps(payload, ensure_ascii=False)
        )
        try:
            r = requests.post(ANTHROPIC_URL, timeout=180, headers={
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            }, json={
                "model": "claude-sonnet-4-6",
                "max_tokens": 8000,
                "messages": [{"role": "user", "content": prompt}],
            })
            if r.status_code != 200:
                print(f"MP themes: HTTP {r.status_code} on batch — skipping")
                continue
            text = "".join(b.get("text", "") for b in r.json().get("content", [])
                           if b.get("type") == "text")
            text = text.strip().removeprefix("```json").removeprefix("```") \
                       .removesuffix("```").strip()
            results = {int(g["id"]): g for g in json.loads(text)
                       if isinstance(g, dict) and g.get("id") is not None}
        except Exception as e:
            print(f"MP themes: batch failed ({e}) — skipping")
            continue
        for m, ex, dx, sig in batch:
            combined = ex + dx
            g = results.get(m["id"])
            if not g or not isinstance(g.get("themes"), list):
                continue
            themes = []
            for t in g["themes"][:3]:
                if not (isinstance(t, dict) and t.get("subject")
                        and t.get("stance")):
                    continue
                th = {"subject": str(t["subject"])[:60],
                      "stance": str(t["stance"])[:120]}
                idx = t.get("eg")
                if isinstance(idx, int) and 0 <= idx < len(combined):
                    th["example"] = combined[idx]["t"]
                themes.append(th)
            if not themes:
                continue
            approach = {k: int(v) for k, v in (g.get("approach") or {}).items()
                        if k in ("pressing", "probing", "constituency",
                                 "supportive") and isinstance(v, (int, float))}
            m["themes"], m["approach"] = themes, approach
            m["themeN"] = len(combined)
            cache[str(m["id"])] = {"sig": sig, "themes": themes,
                                   "approach": approach, "n": len(combined),
                                   "made": date.today().isoformat()}
            done += 1
    save("mp_glosses.json", cache)
    print(f"MP themes: {done}/{len(todo)} generated this run")


# ---------------------------------------------------------------- debates
def _ci(d, *names):
    """Case-tolerant dict get — Hansard's JSON casing is inconsistent."""
    for n in names:
        if n in d:
            return d[n]
        for k in d:
            if k.lower() == n.lower():
                return d[k]
    return None


def _snippet(value_html):
    """Strip a Hansard contribution's HTML down to a short clean extract."""
    import re
    txt = re.sub(r"<[^>]+>", " ", str(value_html or ""))
    txt = html.unescape(re.sub(r"\s+", " ", txt)).strip()
    return txt[:180] if len(txt) >= 60 else ""


def _fulltext(value_html):
    """Full cleaned text of a Hansard contribution, for AI classification.
    Same cleaning as _snippet but keeps the whole speech (capped only at
    CLASS_MAX_CHARS so a single marathon speech can't blow a call)."""
    import re
    txt = re.sub(r"<[^>]+>", " ", str(value_html or ""))
    txt = html.unescape(re.sub(r"\s+", " ", txt)).strip()
    return txt[:CLASS_MAX_CHARS] if len(txt) >= 60 else ""


def fetch_debates(state, debates, mp_q, pending, until=None):
    """Debate sections (Commons Chamber + Westminster Hall) with per-MP
    contribution counts, walked day by day via the Hansard calendar:
    calendar -> sections for day -> section tree -> debate detail.

    Full contribution texts are queued (in spoken order, whole exchange
    per section) into `pending` for AI classification — see
    ai_classify_debates. Nothing shortened at capture time any more.

    Incremental: stored sections are never refetched; nightly runs only
    walk days since the last seen sitting date. Old entries are pruned.
    `until` (YYYY-MM-DD) bounds the walk for history runs; None = today.
    """
    frm = state.get("last_debate_date") or \
        (date.today() - timedelta(days=DEBATE_WINDOW_DAYS)).isoformat()
    today = date.today() if until is None else \
        datetime.strptime(until, "%Y-%m-%d").date()
    print(f"Fetching debate sections since {frm}…")

    def walk(node, out):
        """Collect (ExternalId, Title) from any tree shape, recursively."""
        if isinstance(node, dict):
            ext = title = None
            for k, v in node.items():
                kl = k.lower()
                if kl == "externalid" and v:
                    ext = str(v)
                elif kl == "title" and v:
                    title = html.unescape(str(v)).strip()
            if ext and title:
                out.append((ext, title))
            for v in node.values():
                walk(v, out)
        elif isinstance(node, list):
            for v in node:
                walk(v, out)

    # sitting dates in range, via monthly calendar
    cur = datetime.strptime(frm, "%Y-%m-%d").date().replace(day=1)
    sitting = []
    while cur <= today:
        cal = get(HANSARD_CAL, {"year": cur.year, "month": cur.month,
                                "house": "Commons"})
        for item in (cal or []):
            ds = None
            if isinstance(item, str):
                ds = item[:10]
            elif isinstance(item, dict):
                for k, v in item.items():
                    if "date" in k.lower() and str(v)[:4].isdigit():
                        ds = str(v)[:10]
                        break
            if ds and frm <= ds <= today.isoformat():
                sitting.append(ds)
        cur = date(cur.year + 1, 1, 1) if cur.month == 12 else \
            date(cur.year, cur.month + 1, 1)
    sitting = sorted(set(sitting))
    print(f"  {len(sitting)} sitting days to walk")

    found, fetched, latest = 0, 0, frm
    for ds in sitting:
        secs = get(HANSARD_SECTIONS, {"date": ds, "house": "Commons"}) or []
        for sec in secs:
            sl = str(sec).lower()
            if sl != "debate" and "westminster" not in sl:
                continue  # written statements/answers/petitions etc
            trees = get(HANSARD_TREES, {"section": sec, "date": ds,
                                        "house": "Commons"})
            nodes = []
            walk(trees, nodes)
            for ext, title in nodes:
                found += 1
                if ext in debates:
                    continue
                dj = get(HANSARD_DEBATE.format(ext))
                if not dj:
                    continue
                sp = defaultdict(int)
                items = []
                for item in (_ci(dj, "Items") or []):
                    if not isinstance(item, dict):
                        continue
                    if str(_ci(item, "ItemType") or "") != "Contribution":
                        continue
                    mid = _ci(item, "MemberId")
                    if not mid:
                        continue
                    sp[str(mid)] += 1
                    txt = _fulltext(_ci(item, "Value"))
                    if txt:
                        items.append({"mid": str(mid), "txt": txt})
                if items:
                    pending.append({"ext": ext, "d": ds,
                                    "title": title[:110], "items": items})
                if not sp:
                    continue  # container/heading with no direct speech
                debates[ext] = {"d": ds, "t": title[:110], "sp": dict(sp)}
                fetched += 1
        if ds > latest:
            latest = ds
    # refetch overlap of a couple of days; prune beyond the window
    state["last_debate_date"] = (
        datetime.strptime(latest, "%Y-%m-%d")
        - timedelta(days=2)).date().isoformat() if sitting else frm
    floor = (date.today() - timedelta(days=DEBATE_WINDOW_DAYS)).isoformat()
    for k in [k for k, e in debates.items() if e.get("d", "") < floor]:
        del debates[k]
    print(f"  {found} sections seen, {fetched} new with speech, "
          f"{len(debates)} held")


def ai_tag_debates(debates, headings, depts):
    """Map each untagged debate section onto the site's topic vocabulary
    and a department, via batched API calls. Tags are stored on the entry
    so each section is only ever read once. Procedural business is tagged
    'Procedural' and excluded from momentum downstream."""
    todo = [(k, e) for k, e in debates.items() if "topic" not in e]
    if not todo:
        print("Debate tags: all up to date")
        return
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not key:
        print(f"Debate tags: no key — {len(todo)} sections untagged")
        return
    todo = sorted(todo, key=lambda kv: kv[1].get("d", ""),
                  reverse=True)[:900]  # newest first: current quarter
                                       # must never be the untagged part
    print(f"Debate tags: tagging {len(todo)} sections…")
    done = 0
    for i in range(0, len(todo), 40):
        batch = todo[i:i + 40]
        payload = [{"id": k, "title": e["t"], "date": e["d"]}
                   for k, e in batch]
        prompt = (
            "You are tagging UK House of Commons debate sections for a "
            "parliamentary data site. For each section below, return:\n"
            '- "topic": the policy subject. REUSE one of these existing '
            "topic strings whenever one fits (exact string): "
            + json.dumps(headings[:60]) + ". Only if none fits, coin a "
            'concise topic of 2-5 plain words. Use exactly "Procedural" '
            "for points of order, business of the house, petitions "
            "presentations, and similar non-policy business.\n"
            '- "dept": the most relevant department from this list (exact '
            'string), or "Other": ' + json.dumps(depts) + "\n"
            "Respond with ONLY a JSON array, no markdown fences, of "
            '{"id": "<id>", "topic": "...", "dept": "..."}.\n\n'
            + json.dumps(payload, ensure_ascii=False)
        )
        try:
            r = requests.post(ANTHROPIC_URL, timeout=180, headers={
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            }, json={
                "model": "claude-sonnet-4-6",
                "max_tokens": 8000,
                "messages": [{"role": "user", "content": prompt}],
            })
            if r.status_code != 200:
                print(f"Debate tags: HTTP {r.status_code} on batch — skipping")
                continue
            text = "".join(b.get("text", "") for b in r.json().get("content", [])
                           if b.get("type") == "text")
            text = text.strip().removeprefix("```json").removeprefix("```") \
                       .removesuffix("```").strip()
            tags = {g["id"]: g for g in json.loads(text)
                    if isinstance(g, dict) and g.get("id")}
        except Exception as e:
            print(f"Debate tags: batch failed ({e}) — skipping")
            continue
        for k, e in batch:
            g = tags.get(k)
            if g and g.get("topic"):
                e["topic"] = str(g["topic"])[:60]
                e["dept"] = str(g.get("dept") or "Other")[:60]
                done += 1
    print(f"Debate tags: {done}/{len(todo)} tagged this run")


# ---------------------------------------------------------------- compute
# ------------------------------------------------- speech classification
def ai_classify_debates(pending, mp_q, members):
    """Read every new contribution IN FULL, in the context of its whole
    debate, and store a small verdict on the speaking MP: issue, stance,
    tone, a one/two-sentence summary, and a verbatim receipt quote.
    The AI reads everything; only the verdict is stored.

    stance (anchored to the GOVERNMENT position, never to the previous
    speaker): supporting / pushing further / opposing / seeking /
    constituency.
    tone (judged independently of stance; measured is the default and
    heat must be earned): heated / impassioned / concerned / measured /
    warm.

    Each contribution is read exactly once, the night it is new.
    Unclassified material persists in data/pending_class.json and is
    retried on later nights; anything older than CLASS_PENDING_MAX_DAYS
    degrades to a plain snippet so no contribution is ever lost."""
    if not pending:
        print("Classify: nothing pending")
        return
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not key:
        print(f"Classify: no key — {len(pending)} debates pending")
        _age_out_pending(pending, mp_q)
        return

    def who(mid):
        m = members.get(str(mid)) or {}
        n, p = m.get("name"), m.get("party")
        return f"{n} ({p})" if n else f"Member {mid}"

    # oldest first, chunked with 2 contributions of carried context
    pending.sort(key=lambda d: d.get("d", ""))
    chunks = []
    for deb in pending:
        its = deb["items"]
        step = CLASS_CHUNK
        for s in range(0, len(its), step):
            ctx = max(0, s - 2)
            chunks.append((deb, ctx, s, min(len(its), s + step)))
    chunks = chunks[:CLASS_CHUNK_CAP]
    print(f"Classify: {len(pending)} debates pending, "
          f"running {len(chunks)} chunk(s)…")

    rules = (
        "You are classifying UK House of Commons contributions for a "
        "parliamentary data site. Below is one debate (title, date) and "
        "its contributions in spoken order. Classify EVERY contribution "
        'whose "classify" field is true; the rest are context only.\n'
        "Rules:\n"
        '- stance: exactly one of "supporting", "pushing further", '
        '"opposing", "seeking", "constituency". Stance is ALWAYS judged '
        "against the UK government's position on the matter, never "
        'against the previous speaker. "I support the hon Gentleman" '
        'endorsing a critic of the government is "pushing further" or '
        '"opposing", not "supporting". Use the surrounding contributions '
        "to resolve who and what is being referred to; never label from "
        "agreement words alone. Ministers and frontbenchers answering "
        'for the government are "supporting" unless the text clearly '
        'shows otherwise. "seeking" means genuinely asking or probing '
        'without taking a side. "constituency" means raising a local '
        "case rather than a national position.\n"
        '- tone: exactly one of "heated", "impassioned", "concerned", '
        '"measured", "warm". Judge tone INDEPENDENTLY of stance. '
        '"measured" is the default: Commons language is theatrical by '
        'convention, so reserve "heated" for genuine hostility or '
        'indignation and "impassioned" for real intensity without '
        'hostility. Calm settled opposition is "opposing" + "measured".\n'
        "- issue: the subject in 1-3 plain words (e.g. 'Gaza', "
        "'rail fares', 'school funding'). Use the SAME issue wording for "
        "every contribution about the same subject: near-identical names "
        "('SEND funding' / 'SEND provision' / 'SEND reform') are ONE "
        "issue — pick the broadest natural name (here simply 'SEND') and "
        "reuse it exactly. Specifics belong in the summary, not the "
        "issue name. Singular nouns, no punctuation.\n"
        "- sum: one or two short plain-English sentences on what the "
        "speaker is doing (e.g. 'Welcomes the sanctions but says they "
        "fall short and calls for a full arms embargo.').\n"
        "- q: a quote of AT MOST 25 words copied VERBATIM from that "
        "contribution's text, the words that best justify the stance "
        "and tone together.\n"
        "Respond with ONLY a JSON array, no markdown fences, of "
        '{"i": <index>, "issue": "...", "stance": "...", "tone": "...", '
        '"sum": "...", "q": "..."}.\n\n')

    done_ids, classified = set(), 0
    for deb, ctx, s, e in chunks:
        its = deb["items"]
        if all(its[j].get("done") for j in range(s, e)):
            continue  # this span was fully marked on an earlier run
        contribs = [{"i": j, "who": who(its[j]["mid"]),
                     "classify": j >= s and not its[j].get("done"),
                     "text": its[j]["txt"]}
                    for j in range(ctx, e)]
        payload = {"debate": deb["title"], "date": deb["d"],
                   "contributions": contribs}
        try:
            r = requests.post(ANTHROPIC_URL, timeout=240, headers={
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            }, json={
                "model": "claude-sonnet-4-6",
                "max_tokens": 8000,
                "messages": [{"role": "user", "content":
                              rules + json.dumps(payload,
                                                 ensure_ascii=False)}],
            })
            if r.status_code != 200:
                print(f"Classify: HTTP {r.status_code} — "
                      f"{(r.text or '')[:140]}")
                continue
            text = "".join(b.get("text", "")
                           for b in r.json().get("content", [])
                           if b.get("type") == "text")
            text = text.strip().removeprefix("```json") \
                       .removeprefix("```").removesuffix("```").strip()
            results = {int(g["i"]): g for g in json.loads(text)
                       if isinstance(g, dict) and g.get("i") is not None}
        except Exception as exc:
            print(f"Classify: chunk failed ({exc}) — will retry")
            continue
        for j in range(s, e):
            it = its[j]
            if it.get("done"):
                continue  # already stored on an earlier run — never twice
            g = results.get(j)
            if not g:
                continue
            stance = str(g.get("stance", "")).strip().lower()
            tone = str(g.get("tone", "")).strip().lower()
            if stance not in CLASS_STANCES or tone not in CLASS_TONES:
                continue  # off-menu → stays pending, retried or aged out
            q = str(g.get("q", "")).strip()
            if not q or q not in it["txt"] or len(q.split()) > 30:
                q = it["txt"][:180]  # receipt must be verbatim, else fall back
            mq = mp_q.setdefault(it["mid"], {"total": 0, "months": {},
                                             "bodies": {}})
            mq.setdefault("deb_ex", []).append({
                "d": deb["d"], "t": q,
                "issue": str(g.get("issue", ""))[:40],
                "stance": stance, "tone": tone,
                "sum": str(g.get("sum", ""))[:240],
                "ext": deb["ext"], "dt": deb["title"]})
            it["done"] = True
            classified += 1
        if all(i.get("done") for i in its):
            done_ids.add(deb["ext"])
    # keep only debates with unclassified items; drop finished ones,
    # and inside partially-done debates keep everything (context matters)
    pending[:] = [d for d in pending if d["ext"] not in done_ids]
    _age_out_pending(pending, mp_q)
    print(f"Classify: {classified} contributions classified, "
          f"{len(pending)} debates still pending")


def _age_out_pending(pending, mp_q):
    """Anything unclassified after CLASS_PENDING_MAX_DAYS falls back to
    the old one-snippet-per-MP behaviour so no speech is ever lost."""
    floor = (date.today()
             - timedelta(days=CLASS_PENDING_MAX_DAYS)).isoformat()
    stale = [d for d in pending if d.get("d", "") < floor]
    if not stale:
        return
    for deb in stale:
        seen = set()
        for it in deb["items"]:
            if it.get("done") or it["mid"] in seen:
                continue
            seen.add(it["mid"])
            mq = mp_q.setdefault(it["mid"], {"total": 0, "months": {},
                                             "bodies": {}})
            mq.setdefault("deb_ex", []).append({"d": deb["d"],
                                                "t": it["txt"][:180]})
    pending[:] = [d for d in pending if d.get("d", "") >= floor]
    print(f"Classify: {len(stale)} stale debate(s) aged out to snippets")


def party_majorities(votes, members):
    """For each division, which side did each party's majority take?"""
    out = {}
    for did, d in votes.items():
        tally = defaultdict(lambda: [0, 0])  # party -> [aye, no]
        for mid in d["ayes"]:
            m = members.get(str(mid))
            if m:
                tally[m["party"]][0] += 1
        for mid in d["noes"]:
            m = members.get(str(mid))
            if m:
                tally[m["party"]][1] += 1
        maj = {}
        for p, (a, n) in tally.items():
            tot = a + n
            # 0.85: a genuinely whipped vote moves a party almost as a bloc.
            # Free/conscience votes (e.g. assisted dying, ~60/40 splits)
            # fall below this and so produce no "rebels" — by design.
            if tot >= 10 and max(a, n) / tot >= 0.85:
                maj[p] = "aye" if a > n else "no"
        out[did] = maj
    return out


def compute(members, votes, q_monthly, mp_q, debates):
    # debate aggregates: per-topic momentum buckets, per-MP speaking,
    # per-department speakers — all from contribution counts
    today = date.today()
    cur_months = {(today - timedelta(days=i * 30)).isoformat()[:7] for i in range(3)}
    prev_months = {(today - timedelta(days=(i + 3) * 30)).isoformat()[:7] for i in range(3)}
    deb_agg = defaultdict(lambda: {"cur": 0, "prev": 0})
    dept_speak = defaultdict(lambda: defaultdict(int))
    mp_speak = defaultdict(lambda: defaultdict(int))
    for e in debates.values():
        topic = e.get("topic")
        if not topic or topic == "Procedural":
            continue
        month = e.get("d", "")[:7]
        bucket = "cur" if month in cur_months else \
            "prev" if month in prev_months else None
        n = sum(e.get("sp", {}).values())
        if bucket:
            deb_agg[topic][bucket] += n
        dept = e.get("dept") or ""
        for mid, c in e.get("sp", {}).items():
            mp_speak[mid][topic] += c
            if dept and dept != "Other":
                dept_speak[dept][mid] += c

    print("Computing site data…")
    majors = party_majorities(votes, members)
    ordered = sorted(votes.items(), key=lambda kv: kv[1]["date"])
    recent = [k for k, _ in ordered[-6:]]

    stats = {}
    for mid, m in members.items():
        stats[mid] = {**m, "voted": 0, "rebellions": 0, "form": [],
                      "reb_votes": []}

    for did, d in ordered:
        maj = majors.get(did, {})
        aye_set, no_set = set(map(str, d["ayes"])), set(map(str, d["noes"]))
        for mid, s in stats.items():
            side = "aye" if mid in aye_set else "no" if mid in no_set else None
            if side:
                s["voted"] += 1
                pm = maj.get(s["party"])
                rebelled = pm is not None and side != pm
                if rebelled:
                    s["rebellions"] += 1
                    s["reb_votes"].append(
                        {"d": d["date"], "t": d["title"][:90]})
                if did in recent:
                    s["form"].append("R" if rebelled else "W")
            elif did in recent:
                s["form"].append("A")

    n_div = max(1, len(votes))
    party_reb = defaultdict(list)
    for s in stats.values():
        party_reb[s["party"]].append(s["rebellions"])
    party_avg = {p: (sum(v) / len(v) if v else 0) for p, v in party_reb.items()}

    mps_out = []
    for mid, s in stats.items():
        q = mp_q.get(mid, {"total": 0, "months": {}, "bodies": {}})
        months = sorted(q["months"])[-12:]
        bodies = sorted(q["bodies"].items(), key=lambda kv: -kv[1])[:4]
        bsum = sum(v for _, v in bodies) or 1
        cls = [e for e in q.get("deb_ex", [])
               if isinstance(e, dict) and e.get("stance") in CLASS_STANCES]
        said = None
        if cls:
            cls = sorted(cls, key=lambda e: e.get("d", ""))
            sc, tc = defaultdict(int), defaultdict(int)
            for e in cls:
                sc[e["stance"]] += 1
                tc[e.get("tone", "measured")] += 1
            said = {"n": len(cls), "stance": dict(sc), "tone": dict(tc),
                    "recent": [{"d": e.get("d", ""),
                                "issue": e.get("issue", ""),
                                "stance": e["stance"],
                                "tone": e.get("tone", ""),
                                "sum": e.get("sum", ""),
                                "q": e.get("t", "")}
                               for e in cls[-5:]][::-1]}
        mps_out.append({
            "id": int(mid), "name": s["name"], "party": s["party"],
            "seat": s["seat"],
            "role": s.get("role", ""), "roleType": s.get("roleType", ""),
            "spoken": s.get("spoken", 0),
            "participation": round(100 * s["voted"] / n_div),
            "rebellions": s["rebellions"],
            "rebellionVotes": s["reb_votes"][-12:],
            "partyAvgReb": round(party_avg.get(s["party"], 0), 1),
            "form": s["form"][-6:],
            "questions": q["total"],
            "speaking": sorted(mp_speak.get(mid, {}).items(),
                               key=lambda kv: -kv[1])[:3],
            "spark": [q["months"].get(mo, 0) for mo in months],
            "topics": [[b, round(100 * v / bsum)] for b, v in bodies],
            **({"said": said} if said else {}),
        })
    mps_out.sort(key=lambda m: m["name"])
    ai_mp_themes(mps_out, mp_q)

    # topic momentum: last 90 days vs previous 90 — questions + debate
    # contributions combined (month buckets defined at top of compute)
    agg = defaultdict(lambda: {"cur": 0, "prev": 0, "members": defaultdict(int), "body": ""})
    for month, headings in q_monthly.items():
        bucket = "cur" if month in cur_months else "prev" if month in prev_months else None
        if not bucket:
            continue
        for heading, h in headings.items():
            a = agg[heading]
            a[bucket] += h["n"]
            a["body"] = h.get("body", a["body"])
            for mem, c in h.get("members", {}).items():
                a["members"][mem] += c

    topics_out = []
    for heading in set(agg) | set(deb_agg):
        a = agg.get(heading) or \
            {"cur": 0, "prev": 0, "members": {}, "body": ""}
        d = deb_agg.get(heading, {"cur": 0, "prev": 0})
        cur = a["cur"] + d["cur"]
        prev = a["prev"] + d["prev"]
        if cur < 12:
            continue
        growth = round(100 * (cur - prev) / prev) if prev >= 5 else None
        parties, askers = defaultdict(int), []
        for mem, c in sorted(a["members"].items(), key=lambda kv: -kv[1])[:6]:
            m = members.get(mem)
            if m:
                askers.append({"name": m["name"], "party": m["party"], "n": c})
        for mem, c in a["members"].items():
            m = members.get(mem)
            if m:
                parties[m["party"]] += c
        psum = sum(parties.values()) or 1
        topics_out.append({
            "topic": heading, "body": a["body"],
            "cur": cur, "prev": prev, "growth": growth,
            "qcur": a["cur"], "dcur": d["cur"],
            "parties": sorted(
                [[p, round(100 * c / psum)] for p, c in parties.items()],
                key=lambda x: -x[1])[:5],
            "askers": askers,
        })
    topics_out.sort(key=lambda t: (-(t["growth"] if t["growth"] is not None else -999),
                                   -t["cur"]))
    topics_out = topics_out[:40]
    ai_glosses(topics_out, q_monthly)

    def league(keyfn, n=15, reverse=True):
        rows = sorted(mps_out, key=keyfn, reverse=reverse)[:n]
        return [{"name": r["name"], "party": r["party"], "seat": r["seat"],
                 "v": keyfn(r)} for r in rows]

    leagues = {
        "independence": [
            {"name": r["name"], "party": r["party"], "seat": r["seat"],
             "v": round(r["rebellions"] / max(0.5, r["partyAvgReb"]), 1),
             "raw": r["rebellions"]}
            for r in sorted(mps_out,
                            key=lambda m: -(m["rebellions"] / max(0.5, m["partyAvgReb"]))
                            )[:15] if r["rebellions"] > 0],
        "scrutiny": league(lambda m: m["questions"]),
        "voice": league(lambda m: m["spoken"]),
        "presence": league(lambda m: m["participation"]),    }

    # policy-area leaderboards: top askers per answering department,
    # straight from counts already gathered — no AI involved
    dept_totals = defaultdict(int)
    dept_mps = defaultdict(list)
    name_of = {str(m["id"]): m for m in mps_out}
    for mid, mq in mp_q.items():
        m = name_of.get(mid)
        if not m:
            continue
        for body, n in (mq.get("bodies") or {}).items():
            dept_totals[body] += n
            dept_mps[body].append({"name": m["name"], "party": m["party"],
                                   "seat": m["seat"], "n": n})
    top_depts = sorted(dept_totals, key=lambda b: -dept_totals[b])[:16]
    leagues["policy"] = {
        body: {"total": dept_totals[body],
               "top": sorted(dept_mps[body], key=lambda r: -r["n"])[:10],
               "speakers": [
                   {"name": name_of[mid]["name"],
                    "party": name_of[mid]["party"],
                    "seat": name_of[mid]["seat"], "n": c}
                   for mid, c in sorted(dept_speak.get(body, {}).items(),
                                        key=lambda kv: -kv[1])[:10]
                   if mid in name_of]}
        for body in top_depts}

    save("mps.json", mps_out)
    save("topics.json", topics_out)
    save("leagues.json", leagues)
    save("meta.json", {
        "updated": datetime.utcnow().isoformat() + "Z",
        "mps": len(mps_out), "divisions": len(votes),
        "questions": sum(q["total"] for q in mp_q.values()),
        "sample": False,
    })


# ---------------------------------------------------------------- main
def push_to_db(mp_q):
    """File every newly classified contribution into Supabase (the
    permanent store), on top of the normal JSON files. An entry is
    eligible while it still carries its debate id ('ext'); the id is
    removed here ONLY once Supabase confirms the batch, which makes the
    step exactly-once: successes never re-file (id gone), failures keep
    their id and retry on the next run. The table's unique constraint is
    a second net behind that. No credentials -> skipped, files intact."""
    url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
    if not url or not key:
        print("DB: no Supabase credentials — skipping (files still written)")
        return

    todo = []  # (entry_ref, row) — refs let us strip ids on success
    for mid, e in mp_q.items():
        for x in e.get("deb_ex", []):
            if not (isinstance(x, dict) and x.get("ext")
                    and x.get("stance") in CLASS_STANCES):
                continue
            todo.append((x, {
                "ext": str(x["ext"]),
                "mid": str(mid),
                "said_on": x.get("d", ""),
                "issue": (x.get("issue") or "")[:120],
                "stance": x.get("stance"),
                "tone": x.get("tone"),
                "summary": (x.get("sum") or "")[:400],
                "quote": (x.get("t") or "")[:600],
                "debate": (x.get("dt") or "")[:160],
            }))
    if not todo:
        print("DB: nothing new to file")
        return

    endpoint = f"{url}/rest/v1/speech?on_conflict=ext,mid,said_on,quote"
    headers = {
        "apikey": key,
        "authorization": f"Bearer {key}",
        "content-type": "application/json",
        # ignore-duplicates on that key => rows already present are
        # silently skipped, never errored (safe on overlapping ranges)
        "prefer": "resolution=ignore-duplicates,return=minimal",
    }
    sent, failed = 0, 0
    for i in range(0, len(todo), 500):
        batch = todo[i:i + 500]
        try:
            r = requests.post(endpoint, headers=headers,
                              data=json.dumps([row for _, row in batch]),
                              timeout=120)
            if r.status_code in (200, 201, 204):
                sent += len(batch)
                for entry, _ in batch:          # confirmed: won't re-file
                    entry.pop("ext", None)
                    entry.pop("dt", None)
            else:
                failed += len(batch)
                if failed <= 500:  # print the first failure's reason once
                    print(f"DB: HTTP {r.status_code} — {r.text[:200]}")
        except Exception as exc:
            failed += len(batch)
            print(f"DB: batch failed ({exc})")
    print(f"DB: filed {sent} contributions to Supabase"
          + (f", {failed} failed (will retry next run)" if failed else ""))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["backfill", "nightly"], default="nightly")
    mode = ap.parse_args().mode
    print(f"=== Westminster Engine · {mode} · {datetime.utcnow().isoformat()}Z ===")

    state = load("state.json", {})
    votes = load("votes.json", {})
    q_monthly = load("q_monthly.json", {})
    mp_q = load("mp_q.json", {})
    pending = load("pending_class.json", [])

    if mode == "backfill":
        state, votes, q_monthly, mp_q, pending = {}, {}, {}, {}, []

    members = fetch_members()
    if not members:
        print("FATAL: could not fetch MPs — aborting without touching data.")
        sys.exit(1)

    prev = {str(m.get("id")): m for m in load("mps.json", [])
            if isinstance(m, dict)}
    fetch_roles(members, prev, mode)
    fetch_spoken(members, prev, mode)

    fetch_divisions(state, votes)
    fetch_questions(state, q_monthly, mp_q, mode)

    debates = load("debates.json", {})
    fetch_debates(state, debates, mp_q, pending)
    top_headings = sorted(
        {h for m in q_monthly.values() for h in m},
        key=lambda h: -sum(m.get(h, {}).get("n", 0) for m in q_monthly.values()))
    top_depts_for_ai = sorted(
        {b for mq in mp_q.values() for b in (mq.get("bodies") or {})},
        key=lambda b: -sum((mq.get("bodies") or {}).get(b, 0)
                           for mq in mp_q.values()))[:16]
    ai_tag_debates(debates, top_headings, top_depts_for_ai)
    save("debates.json", debates)

    ai_classify_debates(pending, mp_q, members)
    save("pending_class.json", pending)

    # sweep out accidental double verdicts (same MP, same day, same
    # verbatim quote = the same speech marked twice). Where a pair
    # exists, keep the copy still carrying its debate id so it can
    # file to the database; counts and cards then read clean.
    for mq in mp_q.values():
        dex = mq.get("deb_ex")
        if not dex:
            continue
        seen, keep = {}, []
        for x in dex:
            k = ((x.get("d"), x.get("t"))
                 if isinstance(x, dict) and x.get("t") else None)
            if k is None:
                keep.append(x)
                continue
            if k in seen:
                if "ext" in x and "ext" not in keep[seen[k]]:
                    keep[seen[k]] = x
                continue
            seen[k] = len(keep)
            keep.append(x)
        mq["deb_ex"] = keep

    # drop AI-gloss sample texts from months too old to trend
    keep = {(date.today() - timedelta(days=i * 30)).isoformat()[:7]
            for i in range(5)}
    for month, headings in q_monthly.items():
        if month not in keep:
            for h in headings.values():
                h.pop("ex", None)
    # trim per-MP question texts to the theme window and cap
    theme_floor = (date.today()
                   - timedelta(days=THEME_WINDOW_DAYS + 7)).isoformat()
    for mq in mp_q.values():
        if "deb_ex" in mq:
            mq["deb_ex"] = sorted(
                (e for e in mq["deb_ex"]
                 if isinstance(e, dict) and e.get("d", "") >= theme_floor),
                key=lambda e: e["d"])[-CLASS_KEEP:]
            if not mq["deb_ex"]:
                del mq["deb_ex"]
        if "ex" in mq:
            mq["ex"] = sorted(
                (e for e in mq["ex"]
                 if isinstance(e, dict) and e.get("d", "") >= theme_floor),
                key=lambda e: e["d"])[-THEME_KEEP:]
            if not mq["ex"]:
                del mq["ex"]

    compute(members, votes, q_monthly, mp_q, debates)

    # file newly classified speech into the permanent store (Supabase);
    # push_to_db removes each entry's debate id only on confirmed
    # success, so failures keep their id and retry on the next run
    push_to_db(mp_q)

    save("state.json", state)
    save("votes.json", votes)
    save("q_monthly.json", q_monthly)
    save("mp_q.json", mp_q)
    print("=== done ===")


if __name__ == "__main__":
    main()
