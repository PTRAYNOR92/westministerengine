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
import re
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
CLASS_MAX_CHARS = 4000       # reading limit per contribution (stance and
                             # tone are always evident well before this)
CLASS_CTX_CHARS = 350        # context-only items need just enough text to
                             # resolve references, not the whole speech
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
def ai_classify_debates(pending, mp_q, members, known_issues=None):
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
        + (("- Issue names already on the card index from the last "
            "month, most discussed first: "
            + json.dumps(known_issues, ensure_ascii=False)
            + ". Before coining ANY issue name, check this list; if "
            "the subject matches one, reuse that name EXACTLY, "
            "character for character. Coin a new name only when "
            "nothing on the list covers the subject.\n")
           if known_issues else "")
        +         "- sum: one or two short plain-English sentences on what the "
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
        contribs = []
        for j in range(ctx, e):
            live = j >= s and not its[j].get("done")
            contribs.append({"i": j, "who": who(its[j]["mid"]),
                             "classify": live,
                             "text": its[j]["txt"] if live
                             else its[j]["txt"][:CLASS_CTX_CHARS]})
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



# ---------------------------------------------------------------- bills
# The Bills tab: government legislation only, matched against the
# official roster from Parliament's Bills API so phrases like
# "Energy Bills" (household bills) can never sneak in. Whitespace is
# normalised and Bill/Act-year tails stripped before matching, which
# reunites double-space variants and bills renamed on becoming Acts.
# Entirely self-contained: its own database read, its own API calls,
# and any failure skips the bake and leaves yesterday's bills.json.

BILLS_API = "https://bills-api.parliament.uk/api/v1"
BILLS_MEMBERS_BIO = "https://members-api.parliament.uk/api/Members/{}/Biography"
BILLS_TYPES = (1, 4)      # Government Bill + Hybrid Bill (govt-promoted)
BILLS_FLOOR = 10          # contributions needed to earn a card
BILLS_RECEIPTS = 3        # receipt quotes per bill
BILLS_ASKS = 10           # distinct asks shown as the fault lines
BILLS_REBELS = 6          # backbench-signature amendments shown
BILLS_CHANGES = 8         # agreed amendments shown as actual changes
BILLS_AMEND_PAGE = 100    # API page size; 100 is accepted and faster
BILLS_AMEND_CAP = 900     # per-stage fetch ceiling, politeness cap
BILLS_VERDICTS = 14       # bills sent for a one-line verdict

# Hansard names Finance Bills "(No. 2)", "(No. 3)" within a session but
# the Bills API records each simply as "Finance Act <year>". This maps
# the Hansard stem onto the API stem; dates then pick the right one.
BILLS_STEM_ALIASES = {
    "finance (no. 2)": "finance",
    "finance (no. 3)": "finance",
    "finance (no. 4)": "finance",
}


def _bill_stem(title):
    """Normalise a title to its comparable stem: collapse whitespace,
    drop [HL]/[Lords] markers, strip a trailing Bill / Act <year>,
    straighten curly punctuation, lowercase."""
    t = re.sub(r"\s+", " ", title or "").strip()
    t = re.sub(r"\s*\[(HL|Lords)\]", "", t, flags=re.I)
    t = (t.replace("\u2019", "'").replace("\u2013", "-")
          .replace("\u2014", "-"))
    t = re.sub(r"\s+(Bill|Act(\s+\d{4})?)$", "", t, flags=re.I)
    return t.lower()


_BILLS_SESSION = requests.Session()


def _bills_get(url, params=None):
    """GET with three tries: the Bills API throws the odd 500 and a
    polite retry usually clears it."""
    last = None
    for attempt in range(3):
        try:
            r = _BILLS_SESSION.get(
                url, params=params or {}, timeout=60,
                headers={"User-Agent": "CommonsIndex/1.0"})
            if r.status_code == 200:
                return r.json()
            last = f"HTTP {r.status_code}"
            if r.status_code < 500:
                break
        except Exception as e:
            last = str(e)
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"bills api {last} on {url}")


def fetch_bill_roster():
    """The official list of government and hybrid bills for the current
    session and the two before it (enough to cover this Parliament).
    Returns {stem: [bill, ...]} — a stem can hold several bills, e.g.
    the two Finance Acts of a long session."""
    first = _bills_get(f"{BILLS_API}/Bills", {
        "SortOrder": "DateUpdatedDescending", "Take": 20})
    sessions = {s for b in first.get("items", [])
                for s in (b.get("includedSessionIds") or [])}
    cur = max(sessions) if sessions else None
    if cur is None:
        raise RuntimeError("bills: could not determine current session")
    stems = {}
    for sess in (cur, cur - 1, cur - 2):
        for btype in BILLS_TYPES:
            skip = 0
            while True:
                d = _bills_get(f"{BILLS_API}/Bills", {
                    "BillType": btype, "Session": sess,
                    "Take": 50, "Skip": skip})
                for b in d.get("items", []):
                    key = _bill_stem(b.get("shortTitle"))
                    bucket = stems.setdefault(key, [])
                    if not any(x["billId"] == b["billId"] for x in bucket):
                        bucket.append(b)
                if skip + 50 >= d.get("totalResults", 0):
                    break
                skip += 50
    n = sum(len(v) for v in stems.values())
    print(f"bills: roster {n} government bills, sessions "
          f"{cur - 2}-{cur}")
    return stems


def fetch_bill_speech():
    """Read every classified contribution whose debate title contains
    'bill', straight from the permanent store. Server-side filter keeps
    this read small and separate from the main history read."""
    url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
    if not url or not key:
        print("bills: no Supabase credentials — skipping")
        return []
    rows, offset = [], 0
    while True:
        r = requests.get(
            f"{url}/rest/v1/speech",
            params={"select": "said_on,mid,stance,tone,quote,debate",
                    "debate": "ilike.*bill*",
                    "order": "said_on.asc",
                    "limit": 1000, "offset": offset},
            headers={"apikey": key, "Authorization": f"Bearer {key}"},
            timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"bills: speech read HTTP {r.status_code}")
        page = r.json()
        rows += page
        if len(page) < 1000:
            break
        offset += 1000
    print(f"bills: {len(rows)} bill-debate contributions read")
    return rows


def _match_bill(debate_title, stems):
    """Map a Hansard debate title to a roster stem, or None. The title
    is read up to its first standalone word 'Bill' (so 'Energy Bills'
    never matches), whitespace-normalised, then aliased."""
    t = re.sub(r"\s+", " ", debate_title or "").strip()
    t = (t.replace("\u2019", "'").replace("\u2013", "-")
          .replace("\u2014", "-"))
    m = re.match(r"^(.*?)\s+Bill\b", t, flags=re.I)
    if not m:
        return None
    s = re.sub(r"\s*\[(HL|Lords)\]", "", m.group(1), flags=re.I)
    s = s.lower().strip()
    s = BILLS_STEM_ALIASES.get(s, s)
    return s if s in stems else None


def fetch_bill_stages(bill_id):
    """All stages for one bill, each with its sitting dates. An API
    failure returns [] — that bill ships without spine and markers
    rather than sinking the bake."""
    out, skip = [], 0
    try:
        while True:
            d = _bills_get(f"{BILLS_API}/Bills/{bill_id}/Stages",
                           {"Take": 50, "Skip": skip})
            out += d.get("items", [])
            if skip + 50 >= d.get("totalResults", 0):
                break
            skip += 50
    except Exception as e:
        print(f"bills: stages unavailable for {bill_id} ({e})")
        return []
    return out


def _pick_bill(cands, stage_cache, dates):
    """When one stem holds several bills (the two Finance Acts), pick
    the one whose parliamentary stage dates best bracket the debate
    dates. Newest bill wins any tie."""
    if len(cands) == 1:
        return cands[0]
    mid = sorted(dates)[len(dates) // 2] if dates else ""
    best, best_score = None, None
    for b in sorted(cands, key=lambda x: -x["billId"]):
        st = stage_cache.setdefault(
            b["billId"], fetch_bill_stages(b["billId"]))
        sits = sorted(x["date"][:10] for s in st
                      for x in (s.get("stageSittings") or []))
        if not sits:
            score = 10 ** 6
        elif sits[0] <= mid <= sits[-1]:
            score = 0
        else:
            lo = abs((date.fromisoformat(mid)
                      - date.fromisoformat(sits[0])).days)
            hi = abs((date.fromisoformat(mid)
                      - date.fromisoformat(sits[-1])).days)
            score = min(lo, hi)
        if best_score is None or score < best_score:
            best, best_score = b, score
    return best


_AM_DECISIONS = {
    "agreed": "agreed", "negatived": "negatived",
    "withdrawn": "withdrawn", "notmoved": "notmoved",
    "nodecision": "tabled",
}

# The Bills API writes parties out in full; the site speaks in codes.
BILLS_PARTY_CODES = {
    "Conservative": "Con", "Labour": "Lab", "Labour (Co-op)": "Lab",
    "Liberal Democrat": "LD", "Scottish National Party": "SNP",
    "Democratic Unionist Party": "DUP", "Ulster Unionist Party": "UUP",
    "Social Democratic & Labour Party": "SDLP",
    "Green Party": "Green", "Plaid Cymru": "PC",
    "Sinn F\u00e9in": "SF", "Alliance": "APNI",
    "Reform UK": "RUK", "Traditional Unionist Voice": "TUV",
    "Independent": "Ind",
}



def _supa(path, method="get", **kw):
    """One door to Supabase for the bills work. Writes need the master
    key, which only the nightly job holds."""
    url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
    if not url or not key:
        raise RuntimeError("no Supabase credentials")
    h = {"apikey": key, "Authorization": f"Bearer {key}",
         "Content-Type": "application/json"}
    h.update(kw.pop("headers", {}))
    r = getattr(requests, method)(f"{url}/rest/v1/{path}",
                                  headers=h, timeout=60, **kw)
    if r.status_code >= 300:
        raise RuntimeError(f"supabase {method} {path} "
                           f"HTTP {r.status_code}: {r.text[:200]}")
    return r


def _chunk(rows, size=500):
    for i in range(0, len(rows), size):
        yield rows[i:i + size]


def store_bill_debate(links):
    """Write down which debate title belongs to which bill, so the
    match is a recorded decision rather than a nightly guess. Rows are
    left alone once written; a title that stops matching stays on the
    shelf for inspection rather than vanishing."""
    if not links:
        return
    rows = [{"debate_title": t, "bill_id": b["billId"],
             "bill_title": b.get("shortTitle", ""), "confirmed": True}
            for t, b in links.items()]
    for c in _chunk(rows):
        _supa("bill_debate", "post", json=c,
              headers={"Prefer": "resolution=merge-duplicates"})
    print(f"bills: {len(rows)} debate titles filed against bills")


def store_amendments(bill_id, amend_rows, today):
    """File amendments, their signatures, and one diary line per
    amendment per day the signature count changes. The diary is what
    later shows names arriving on an amendment over a fortnight."""
    if not amend_rows:
        return
    main, sponsors, history = [], [], []
    for a in amend_rows:
        main.append({
            "bill_id": bill_id, "house": a["house"], "dnum": a["dnum"],
            "ask": a["t"], "lead_name": a["lead"],
            "lead_party": a["lp"], "names": a["n"],
            "parties": a["parties"], "decision": a["dec"],
            "stage": a["stage"], "clause": a["clause"],
            "last_seen": today,
        })
        for mid in a["mids"]:
            sponsors.append({"bill_id": bill_id, "house": a["house"],
                             "dnum": a["dnum"], "mid": str(mid)})
        history.append({"bill_id": bill_id, "house": a["house"],
                        "dnum": a["dnum"], "seen_on": today,
                        "names": a["n"]})
    for c in _chunk(main):
        _supa("amendment", "post", json=c,
              headers={"Prefer": "resolution=merge-duplicates"})
    for c in _chunk(sponsors):
        _supa("amendment_sponsor", "post", json=c,
              headers={"Prefer": "resolution=ignore-duplicates"})
    for c in _chunk(history):
        _supa("amendment_history", "post", json=c,
              headers={"Prefer": "resolution=merge-duplicates"})


def read_amendments(bill_id, members, gov_party):
    """Read a bill's amendments back out of the permanent store and
    shape them for the page. Passed bills are served entirely from here
    and never troubled the Bills API again."""
    rows, offset = [], 0
    while True:
        r = _supa("amendment", "get", params={
            "select": "house,dnum,ask,ask_ai,lead_name,lead_party,"
                      "names,parties,decision,stage,clause",
            "bill_id": f"eq.{bill_id}",
            "order": "names.desc", "limit": 1000, "offset": offset})
        page = r.json()
        rows += page
        if len(page) < 1000:
            break
        offset += 1000
    if not rows:
        return None
    sp, offset = {}, 0
    while True:
        r = _supa("amendment_sponsor", "get", params={
            "select": "house,dnum,mid", "bill_id": f"eq.{bill_id}",
            "limit": 1000, "offset": offset})
        page = r.json()
        for x in page:
            sp.setdefault((x["house"], x["dnum"]), []).append(x["mid"])
        if len(page) < 1000:
            break
        offset += 1000
    out = []
    for a in rows:
        gb = []
        for mid in sp.get((a["house"], a["dnum"]), []):
            m = members.get(str(mid)) or {}
            if (a["house"] == "Commons" and m.get("party") == gov_party
                    and m.get("roleType") != "gov"):
                gb.append(m.get("name") or "")
        lead_gov = False
        out.append({"t": a["ask"], "ask_ai": a.get("ask_ai") or "",
                    "dnum": a["dnum"], "lead_gov": lead_gov,
                    "n": a["names"] or 0,
                    "parties": a["parties"] or {},
                    "lead": a["lead_name"] or "", "lp": a["lead_party"] or "",
                    "gb": [x for x in gb if x], "dec": a["decision"],
                    "house": a["house"], "stage": a["stage"],
                    "clause": a["clause"]})
    return _shape_amendments(out, gov_party)


def _with_backbench(flat, members, gov_party):
    """Mark which signatures came from governing-party backbenchers."""
    out = []
    for a in flat:
        gb = []
        for mid in a.get("mids", []):
            m = members.get(str(mid)) or {}
            if (a["house"] == "Commons" and m.get("party") == gov_party
                    and m.get("roleType") != "gov"):
                gb.append(m.get("name") or "")
        lead_gov = False
        for mid in a.get("mids", [])[:1]:
            lead_gov = (members.get(str(mid)) or {}).get(
                "roleType") == "gov"
        out.append({**a, "gb": [x for x in gb if x],
                    "lead_gov": lead_gov})
    return out


def _shape_amendments(all_am, gov_party):
    """Turn a flat list of amendments into what the page shows: the
    asks ranked by signatures, and the governing-party backbench
    signatures that are the early warning of a rebellion."""
    cm = [a for a in all_am if a["house"] == "Commons"]
    ld = [a for a in all_am if a["house"] == "Lords"]
    decided = {}
    for a in cm:
        if a["dec"] != "tabled":
            decided[a["dec"]] = decided.get(a["dec"], 0) + 1
    best = {}
    for a in sorted(all_am, key=lambda x: -x["n"]):
        k = (a["t"] or "").lower()[:60]
        if k and k not in best:
            best[k] = a
    asks = sorted(best.values(), key=lambda a: -a["n"])[:BILLS_ASKS]
    rebels = sorted(
        [a for a in cm if a["gb"] and (a["lp"] != gov_party
                                       or len(a["gb"]) >= 3)],
        key=lambda a: (a["lp"] == gov_party, -len(a["gb"]),
                       -a["n"]))[:BILLS_REBELS]
    # Amendments that were agreed to are the ones that actually
    # changed the bill. They rarely carry many names, because they are
    # usually tabled by the minister, so they never surface in a list
    # ranked by signatures and need their own section.
    agreed = [a for a in cm if a["dec"] == "agreed"]
    gov_led = sum(1 for a in agreed
                  if a.get("lead_gov") or a["lp"] == gov_party)
    changes = sorted(agreed, key=lambda a: (-a["n"],
                                            a.get("clause") or 999))
    return {
        "commons": {"n": len(cm), "decided": decided,
                    "agreed_n": len(agreed), "agreed_gov": gov_led},
        "changes": [{"t": a["t"], "ask_ai": a.get("ask_ai", ""),
                     "dnum": a["dnum"], "house": a["house"],
                     "n": a["n"], "lead": a["lead"], "lp": a["lp"],
                     "stage": a["stage"], "clause": a.get("clause")}
                    for a in changes[:BILLS_CHANGES]],
        "lords": {"n": len(ld)},
        "asks": [{k: v for k, v in a.items()
                  if k not in ("gb", "raw", "mids")} for a in asks],
        "rebels": [{"t": a["t"], "ask_ai": a.get("ask_ai", ""),
                    "n": a["n"], "lead": a["lead"],
                    "lp": a["lp"], "gb": a["gb"][:8],
                    "gbn": len(a["gb"]), "dec": a["dec"],
                    "stage": a["stage"]} for a in rebels],
    }


def _clean_ask(a):
    """The plain-English ask behind an amendment. New clauses carry a
    title in bold, which is Parliament's own words for what the fight
    is about. Older-style amendments to existing text have no title, so
    their instruction is used instead."""
    txt = " ".join(a.get("summaryText") or [])
    m = re.search(r"<b>\s*[\u201c\"\']?(.+?)[\u201d\"\']?\s*</b>",
                  txt, flags=re.S)
    t = m.group(1) if m else txt
    t = re.sub(r"<[^>]+>", " ", t)
    t = re.sub(r"\s+", " ", t).strip(" ,.;:\u2014-\u201c\u201d\"\'")
    return t[:300]


def fetch_bill_amendments(bill_id, stages, members, gov_party):
    """Amendments across both Houses, read for signatures rather than
    counts. Most amendments are never moved or voted on, so the
    decision field says little; the number of members who put their
    name to one, and which benches they sit on, says a great deal.

    The same amendment is relisted with a fresh id when a bill is
    carried over, so records are pooled on dNum, its number on the
    marshalled list, which is stable across stages."""
    AMENDABLE = ("committee", "report", "reintroduced",
                 "consideration", "amendments")
    seen = {}
    for s in stages:
        house = s.get("house") or ""
        if house not in ("Commons", "Lords"):
            continue
        if not any(k in (s.get("description") or "").lower()
                   for k in AMENDABLE):
            continue
        skip, got = 0, 0
        while True:
            try:
                d = _bills_get(
                    f"{BILLS_API}/Bills/{bill_id}/Stages/{s['id']}"
                    f"/Amendments",
                    {"Take": BILLS_AMEND_PAGE, "Skip": skip})
            except Exception:
                break                     # stage has no amendment list
            items = d.get("items", [])
            if not items:
                break
            for i, a in enumerate(items):
                sponsors = a.get("sponsors") or []
                parties, gb = {}, []
                for sp in sponsors:
                    mid = str(sp.get("memberId") or "")
                    m = members.get(mid) or {}
                    p = m.get("party") or BILLS_PARTY_CODES.get(
                        sp.get("party", ""), sp.get("party", "") or "?")
                    parties[p] = parties.get(p, 0) + 1
                    if (house == "Commons" and p == gov_party
                            and m.get("roleType") != "gov"):
                        gb.append(m.get("name") or sp.get("name", ""))
                lead = next((sp for sp in sponsors if sp.get("isLead")),
                            sponsors[0] if sponsors else {})
                lmid = str(lead.get("memberId") or "")
                lp = (members.get(lmid) or {}).get(
                    "party") or BILLS_PARTY_CODES.get(
                    lead.get("party", ""), lead.get("party", ""))
                dnum = str(a.get("dNum") or a.get("amendmentId")
                           or f"{s['id']}:{skip+i}")
                seen[(house, dnum)] = {
                    "dnum": dnum,
                    "raw": re.sub(r"\s+", " ", re.sub(
                        r"<[^>]+>", " ",
                        " ".join(a.get("summaryText") or [])))[:700],
                    "mids": [str(sp.get("memberId") or "")
                             for sp in sponsors if sp.get("memberId")],
                    "t": _clean_ask(a),
                    "n": len(sponsors),
                    "parties": parties,
                    "lead": lead.get("name", ""),
                    "lp": lp,
                    "gb": gb,
                    "dec": _AM_DECISIONS.get(
                        str(a.get("decision") or "NoDecision").lower(),
                        "tabled"),
                    "house": house,
                    "stage": s.get("description", ""),
                    "clause": a.get("clause"),
                }
            got += len(items)
            skip += BILLS_AMEND_PAGE
            if skip >= d.get("totalResults", 0) or got >= BILLS_AMEND_CAP:
                break

    return list(seen.values())


def _fetch_gov_windows(mids):
    """Government post date windows for the given member ids, from the
    Members API biography record: {mid: [(start, end_or_None)]}. Only
    governing-party members who actually spoke on a bill are looked up.
    A member missing from the result fell back on the day; bench then
    uses their current role instead."""
    import concurrent.futures as _cf
    dead = []

    def one(mid):
        if dead:
            return mid, None      # API unreachable; stop asking
        try:
            j = get(BILLS_MEMBERS_BIO.format(mid))
        except Exception:
            j = None
        if not j:
            dead.append(1)
            return mid, None
        posts = ((j.get("value") or {}).get("governmentPosts")) or []
        wins = []
        for p in posts:
            if isinstance(p, dict) and p.get("startDate"):
                wins.append((str(p["startDate"])[:10],
                             str(p.get("endDate") or "")[:10] or None))
        return mid, wins

    out = {}
    with _cf.ThreadPoolExecutor(max_workers=6) as pool:
        for mid, wins in pool.map(one, mids):
            if wins is not None:
                out[mid] = wins
    return out



# An amendment that adds something new to a bill is given a title by
# Parliament and reads plainly. An amendment that edits wording already
# in the bill is published only as an instruction to the printer, which
# tells a reader nothing. Those are the ones worth summarising.
_TECHNICAL = re.compile(r"^(Clause|Page|Schedule|Line)\s", re.I)


def summarise_asks(bill_id, shown):
    """Give the unreadable amendments one plain sentence each. Every
    summary is written once and filed next to the amendment, so the
    same amendment is never paid for twice; only new ones cost
    anything. Failure leaves the drafting text in place."""
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    todo = [a for a in shown
            if _TECHNICAL.match(a.get("t") or "")
            and not (a.get("ask_ai") or "").strip()
            and (a.get("raw") or a.get("t"))]
    if not key or not todo:
        return
    payload = [{"id": f'{a["house"]}:{a["dnum"]}',
                "text": (a.get("raw") or a["t"])[:700]} for a in todo]
    prompt = (
        "Below are amendments tabled to a bill in the UK Parliament, as "
        "published: instructions to insert or remove words at a given "
        "line of the bill.\n"
        "For each one write ONE sentence, under 20 words, saying what "
        "the amendment would do in practice. Start with 'Would'. Plain "
        "English, no jargon, no legal citation, no em dashes.\n"
        "State only what the text itself does. Do not say whether it is "
        "strengthening, weakening, tough or modest, and do not guess at "
        "motive. If the text is too fragmentary to tell, reply with an "
        "empty string for that id.\n"
        "Reply with ONLY a JSON object mapping each id to its sentence. "
        "No other text.\n\n" + json.dumps(payload, ensure_ascii=False))
    try:
        r = requests.post(ANTHROPIC_URL, timeout=180, headers={
            "x-api-key": key, "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }, json={"model": "claude-sonnet-4-6", "max_tokens": 3000,
                 "messages": [{"role": "user", "content": prompt}]})
        txt = "".join(b.get("text", "")
                      for b in r.json().get("content", []))
        txt = (txt.strip().removeprefix("```json").removeprefix("```")
               .removesuffix("```").strip())
        got = json.loads(txt)
    except Exception as e:
        print(f"bills: amendment summaries skipped ({e})")
        return
    rows = []
    for a in todo:
        s = got.get(f'{a["house"]}:{a["dnum"]}')
        if isinstance(s, str) and s.strip():
            a["ask_ai"] = (s.strip().replace("\u2014", ",")
                           .replace("\u2013", ","))
            rows.append({"bill_id": bill_id, "house": a["house"],
                         "dnum": a["dnum"], "ask_ai": a["ask_ai"]})
    if rows:
        try:
            for c in _chunk(rows):
                _supa("amendment", "post", json=c,
                      headers={"Prefer": "resolution=merge-duplicates"})
        except Exception as e:
            print(f"bills: could not file summaries ({e})")
    print(f"bills: {len(rows)} amendments summarised for bill {bill_id}")


def _ai_bill_verdicts(cards):
    """One small call: a plain-English verdict per bill, grounded only
    in the numbers and quotes supplied. Failures are silent — the tab
    simply ships without verdicts."""
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    top = cards[:BILLS_VERDICTS]
    if not key or not top:
        return
    payload = [{"bill": c["title"], "stage": c["stage"],
                "house": c["house"], "contributions": c["n"],
                "heat": c["heat"], "stances": c["stances"],
                "tones": c["tones"],
                "quotes": [r["q"] for r in c["receipts"]][:2]}
               for c in top]
    prompt = (
        "You write one-line verdicts for pages tracking government "
        "bills through the House of Commons. For each bill below you "
        "get its current stage, how many classified Commons "
        "contributions it has drawn, its heat score (0 calm to 3 "
        "furious), the stance mix relative to the government position, "
        "the tone mix, and sample quotes.\n"
        "Write ONE sentence per bill, under 22 words, in plain "
        "newspaper English, describing how the Commons is receiving "
        "it. State only what the supplied numbers and quotes support. "
        "Never use em dashes. No hype words.\n"
        "Reply with ONLY a JSON object mapping each bill title exactly "
        "as given to its sentence. No other text.\n\n"
        + json.dumps(payload, ensure_ascii=False))
    try:
        r = requests.post(ANTHROPIC_URL, timeout=120, headers={
            "x-api-key": key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }, json={
            "model": "claude-sonnet-4-6",
            "max_tokens": 1400,
            "messages": [{"role": "user", "content": prompt}],
        })
        txt = "".join(b.get("text", "") for b in r.json().get("content", []))
        txt = (txt.strip().removeprefix("```json").removeprefix("```")
               .removesuffix("```").strip())
        verdicts = json.loads(txt)
    except Exception as e:
        print(f"bills: verdicts skipped ({e})")
        return
    for c in cards:
        v = verdicts.get(c["title"])
        if isinstance(v, str) and v.strip():
            c["verdict"] = (v.strip().replace("\u2014", ",")
                            .replace("\u2013", ","))
    print(f"bills: verdicts written for "
          f"{sum(1 for c in cards if c.get('verdict'))} bills")


def export_bills(members):
    """Build data/bills.json: one card per government bill with Commons
    sentiment behind it. Verdict, party mood board (government
    frontbench and backbench separated), heat timeline with stage
    markers, journey spine, amendments traction ladder, receipts."""
    # Amendment lists for bills that have already passed cannot
    # change, so yesterday's bake is reused and the API spared.
    old_am = {}
    try:
        with open(os.path.join(DATA, "bills.json"), encoding="utf-8") as f:
            for c in json.load(f).get("bills", []):
                if c.get("act") and c.get("amendments", {}).get("asks"):
                    old_am[c["id"]] = c["amendments"]
    except Exception:
        pass
    if old_am:
        print(f"bills: reusing amendments for {len(old_am)} passed bills")

    stems = fetch_bill_roster()
    rows = fetch_bill_speech()
    if not rows:
        return

    tally = {}
    for m in members.values():
        if m.get("roleType") == "gov":
            tally[m.get("party")] = tally.get(m.get("party"), 0) + 1
    gov_party = max(tally, key=tally.get) if tally else "Lab"

    grouped, links = {}, {}
    for r in rows:
        s = _match_bill(r.get("debate"), stems)
        if s:
            grouped.setdefault(s, []).append(r)
            if r.get("debate") not in links:
                links[r["debate"]] = max(stems[s],
                                         key=lambda b: b["billId"])
    try:
        store_bill_debate(links)
    except Exception as e:
        print(f"bills: could not file debate links ({e})")

    # bench at the time of speaking: government post windows for every
    # governing-party member who spoke on any bill. Failure falls back
    # to current roles rather than sinking the bake.
    gov_mids = sorted({str(r.get("mid")) for g in grouped.values()
                       for r in g
                       if (members.get(str(r.get("mid"))) or {}
                           ).get("party") == gov_party})
    gov_windows = {}
    try:
        gov_windows = _fetch_gov_windows(gov_mids)
        print(f"bills: post history for {len(gov_windows)} of "
              f"{len(gov_mids)} governing-party speakers")
    except Exception as e:
        print(f"bills: post history unavailable ({e}) — current roles")

    def _was_minister(mid, on_date):
        wins = gov_windows.get(mid)
        if wins is None:
            return (members.get(mid) or {}).get("roleType") == "gov"
        return any(s <= on_date and (e is None or on_date <= e)
                   for s, e in wins)

    # which session Parliament is currently in, used to tell a live
    # bill from one that fell when the last session ended
    cur_session = 0
    try:
        cur_session = max(s for bucket in stems.values() for b in bucket
                          for s in (b.get("includedSessionIds") or []))
    except Exception:
        pass

    def _build_card(s, srows):
        dates = sorted({r["said_on"] for r in srows
                        if r.get("said_on")})
        cache = {}
        bill = _pick_bill(stems[s], cache, dates)
        stages = cache.get(bill["billId"]) or fetch_bill_stages(
            bill["billId"])

        today_s = date.today().isoformat()

        # per-day heat timeline
        by_day = {}
        for r in srows:
            d = r.get("said_on") or ""
            a = by_day.setdefault(d, [0, 0])
            a[0] += 1
            a[1] += HEAT_W.get(r.get("tone") or "", 0)
        timeline = [{"d": d, "n": a[0], "heat": round(a[1] / a[0], 2)}
                    for d, a in sorted(by_day.items())]

        # journey spine + stage markers, in sitting-date order
        cur_id = (bill.get("currentStage") or {}).get("id")
        spine = []
        for st in stages:
            sits = sorted(x["date"][:10]
                          for x in (st.get("stageSittings") or []))
            if st["id"] == cur_id:
                status = "current"
            elif not sits:
                status = "upcoming"
            elif sits[-1] < today_s:
                status = "done"
            else:
                status = "upcoming"
            spine.append({"stage": st.get("description", ""),
                          "house": st.get("house", ""),
                          "dates": sits, "status": status,
                          "sort": (sits[0] if sits else "9999",
                                   st.get("sortOrder", 99))})
        spine.sort(key=lambda x: x["sort"])
        for x in spine:
            x.pop("sort", None)
        markers = [{"d": x["dates"][0],
                    "s": x["stage"], "h": x["house"]}
                   for x in spine if x["dates"]]

        # party mood board, government split front / back
        mood = {}
        tones, stances = {}, {}
        recs = []
        for r in srows:
            tone = r.get("tone") or ""
            stance = r.get("stance") or ""
            tones[tone] = tones.get(tone, 0) + 1
            stances[stance] = stances.get(stance, 0) + 1
            m = members.get(str(r.get("mid"))) or {}
            party = m.get("party", "?")
            if party == gov_party:
                bench = ("Government frontbench"
                         if _was_minister(str(r.get("mid")),
                                          r.get("said_on") or "")
                         else "Government backbench")
            else:
                bench = party
            a = mood.setdefault(bench, {"party": party, "n": 0,
                                        "tones": {}, "stances": {},
                                        "pts": 0})
            a["n"] += 1
            a["pts"] += HEAT_W.get(tone, 0)
            a["tones"][tone] = a["tones"].get(tone, 0) + 1
            a["stances"][stance] = a["stances"].get(stance, 0) + 1
            w = HEAT_W.get(tone, 0)
            if w >= 2 and r.get("quote"):
                recs.append((w, r.get("said_on") or "", {
                    "who": m.get("name", "an MP"),
                    "party": party, "tone": tone,
                    "d": r.get("said_on") or "",
                    "q": str(r.get("quote"))[:240]}))
        mood_rows = []
        for bench, a in mood.items():
            mood_rows.append({
                "bench": bench, "party": a["party"], "n": a["n"],
                "heat": round(a["pts"] / a["n"], 2),
                "tones": a["tones"], "stances": a["stances"]})
        mood_rows.sort(key=lambda x: -x["n"])
        receipts = [r for _, _, r in
                    sorted(recs, key=lambda x: (-x[0], x[1]))
                    ][:BILLS_RECEIPTS]

        # Amendments: live bills are checked against Parliament and the
        # result filed; passed bills are served from the store, since a
        # bill that is law cannot gain amendments.
        am = None
        try:
            if bill.get("isAct"):
                am = read_amendments(bill["billId"], members, gov_party)
            if am is None:
                flat = fetch_bill_amendments(bill["billId"], stages,
                                             members, gov_party)
                try:
                    store_amendments(bill["billId"], flat, today_s)
                except Exception as e:
                    print(f"bills: could not file amendments for "
                          f"{bill['billId']} ({e})")
                am = _shape_amendments(_with_backbench(flat, members,
                                                       gov_party),
                                       gov_party)
                # only amendments the page shows are worth summarising
                by_key = {(x["house"], x["dnum"]): x for x in flat}
                shown = [by_key[(a["house"], a["dnum"])]
                         for a in am["asks"] + am.get("changes", [])
                         if (a["house"], a["dnum"]) in by_key]
                summarise_asks(bill["billId"], shown)
                for a in am["asks"] + am.get("changes", []):
                    src_a = by_key.get((a["house"], a["dnum"]))
                    if src_a and src_a.get("ask_ai"):
                        a["ask_ai"] = src_a["ask_ai"]
        except Exception as e:
            print(f"bills: amendments unavailable for "
                  f"{bill['billId']} ({e})")
            am = {"commons": {"n": 0, "decided": {}},
                  "lords": {"n": 0}, "asks": [], "rebels": []}

        pts = sum(HEAT_W.get(t, 0) * c for t, c in tones.items())
        cur = bill.get("currentStage") or {}

        # Three states, not two. A bill that never passed and was never
        # voted down has not necessarily survived: if it did not finish
        # before the session ended, it fell.
        sessions = bill.get("includedSessionIds") or []
        if bill.get("isAct"):
            status = "act"
        elif bill.get("isDefeated"):
            status = "fell"
        elif cur_session and cur_session in sessions:
            status = "live"
        else:
            status = "fell"

        assent = ""
        if status == "act":
            for st in stages:
                if "Royal Assent" in (st.get("description") or ""):
                    sits = sorted(x["date"][:10]
                                  for x in (st.get("stageSittings") or []))
                    if sits:
                        assent = sits[0]
                    break

        return {
            "id": bill["billId"],
            "status": status,
            "assent": assent,
            "title": re.sub(r"\s+Act\s+\d{4}$", " Bill",
                            bill.get("shortTitle", "")),
            "act": bool(bill.get("isAct")),
            "stage": cur.get("description", ""),
            "house": cur.get("house", ""),
            "n": len(srows),
            "heat": round(pts / len(srows), 2),
            "tones": tones, "stances": stances,
            "first": dates[0] if dates else "",
            "last": dates[-1] if dates else "",
            "timeline": timeline, "markers": markers,
            "spine": spine, "mood": mood_rows,
            "amendments": am,
            "receipts": receipts,
        }

    import concurrent.futures as _cf
    jobs = [(s, srows) for s, srows in grouped.items()
            if len(srows) >= BILLS_FLOOR]
    cards = []
    with _cf.ThreadPoolExecutor(max_workers=4) as pool:
        futs = {pool.submit(_build_card, s, srows): s
                for s, srows in jobs}
        for f in _cf.as_completed(futs):
            try:
                cards.append(f.result())
                print(f"bills: built {futs[f]}", flush=True)
            except Exception as e:
                print(f"bills: skipped one bill ({futs[f]}: {e})")

    # Government bills before Parliament that the Commons has not yet
    # debated. They are named rather than left out silently, so the tab
    # accounts for every live bill on Parliament's own list and the
    # reader can see exactly where the boundary sits.
    matched_ids = set()
    for s in grouped:
        matched_ids.add(max(stems[s], key=lambda b: b["billId"])["billId"])
    undebated = []
    try:
        cur = cur_session
        for bucket in stems.values():
            for b in bucket:
                if (b["billId"] in matched_ids or b.get("isAct")
                        or b.get("isDefeated")
                        or cur not in (b.get("includedSessionIds") or [])):
                    continue
                cs = b.get("currentStage") or {}
                undebated.append({
                    "title": b.get("shortTitle", ""),
                    "house": cs.get("house", ""),
                    "stage": cs.get("description", "")})
        undebated.sort(key=lambda x: x["title"])
    except Exception as e:
        print(f"bills: undebated list skipped ({e})")

    # The tab shows bills still going through Parliament. Bills that
    # have passed stay in the store and stay available behind the
    # archive switch, so a bill does not vanish the day it becomes law.
    live = [c for c in cards if c["status"] == "live"]
    passed = [c for c in cards if c["status"] == "act"]
    fell = [c for c in cards if c["status"] == "fell"]
    for group in (live, passed, fell):
        group.sort(key=lambda c: (c["last"], c["n"]), reverse=True)
    cards = live + passed + fell
    _ai_bill_verdicts(live)
    save("bills.json", {
        "updated": date.today().isoformat(),
        "gov_party": gov_party,
        "floor": BILLS_FLOOR,
        "live": len(live),
        "fell": len(fell),
        "undebated": undebated,
        "bills": cards,
    })
    print(f"bills: exported {len(live)} live, {len(passed)} passed, "
          f"{len(fell)} fell, {len(undebated)} live but not yet "
          f"debated in the Commons")

# ------------------------------------------------------------- heat board

# tone weights: the heat formula. Heat is earned, calm scores nothing.
HEAT_W = {"heated": 3, "impassioned": 2, "concerned": 1,
          "measured": 0, "warm": 0}
HEAT_FLOOR_FORT = 10     # min contributions to appear (10 sitting days)
HEAT_FLOOR_QTR = 20      # min contributions to appear (quarter)
HEAT_HISTORY_DAYS = 400  # how far back to read for windows + trends
HEAT_RECEIPTS = 3        # receipt quotes per issue
HEAT_VERDICTS = 12       # issues sent for a one-line verdict



def fetch_issue_aliases():
    """Read the issue_alias card index (variant -> canonical) so
    splintered issue names pool together. Empty dict on any failure:
    the board still builds, just without the merges."""
    url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
    if not url or not key:
        return {}
    try:
        r = requests.get(f"{url}/rest/v1/issue_alias",
                         params={"select": "variant,canonical",
                                 "limit": 1000},
                         headers={"apikey": key,
                                  "Authorization": f"Bearer {key}"},
                         timeout=60)
        if r.status_code != 200:
            print(f"heat: alias read failed HTTP {r.status_code}")
            return {}
        aliases = {row["variant"]: row["canonical"] for row in r.json()
                   if row.get("variant") and row.get("canonical")}
        print(f"heat: {len(aliases)} issue aliases loaded")
        return aliases
    except Exception as e:
        print(f"heat: alias read error {e}")
        return {}


def fetch_recent_issue_names(aliases):
    """Top issue names from the last 30 days, canonicalised, most
    frequent first. Fed into the classification prompt so tonight's
    labels reuse the existing card index instead of splintering."""
    url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
    if not url or not key:
        return []
    since = (date.today() - timedelta(days=30)).isoformat()
    counts, offset = {}, 0
    while True:
        try:
            r = requests.get(f"{url}/rest/v1/speech",
                             params={"select": "issue",
                                     "said_on": f"gte.{since}",
                                     "limit": 1000, "offset": offset},
                             headers={"apikey": key,
                                      "Authorization": f"Bearer {key}"},
                             timeout=60)
            if r.status_code != 200:
                print(f"classify: issue-name read failed HTTP {r.status_code}")
                return []
            page = r.json()
        except Exception as e:
            print(f"classify: issue-name read error {e}")
            return []
        for row in page:
            nm = aliases.get(row.get("issue"), row.get("issue"))
            if nm:
                counts[nm] = counts.get(nm, 0) + 1
        if len(page) < 1000:
            break
        offset += 1000
    names = sorted(counts, key=counts.get, reverse=True)[:150]
    print(f"classify: {len(names)} recent issue names for the prompt")
    return names


def fetch_speech_history():
    """Read recent classified speech back from Supabase, oldest first.

    Paginates 1,000 rows at a time. Returns [] (never crashes the run)
    if the database is unreachable, so the site simply keeps yesterday's
    heat.json.
    """
    url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
    if not url or not key:
        print("heat: no Supabase credentials — skipping heat export")
        return []
    since = (date.today() - timedelta(days=HEAT_HISTORY_DAYS)).isoformat()
    rows, offset = [], 0
    while True:
        try:
            r = requests.get(
                f"{url}/rest/v1/speech",
                params={"select": "said_on,issue,stance,tone,quote,mid",
                        "said_on": f"gte.{since}",
                        "order": "said_on.asc",
                        "limit": 1000, "offset": offset},
                headers={"apikey": key, "Authorization": f"Bearer {key}"},
                timeout=60)
            if r.status_code != 200:
                print(f"heat: read failed HTTP {r.status_code} — skipping")
                return []
            page = r.json()
        except Exception as e:
            print(f"heat: read error {e} — skipping")
            return []
        rows.extend(page)
        if len(page) < 1000:
            break
        offset += 1000
    print(f"heat: read {len(rows)} classified contributions since {since}")
    return rows


def _issue_key(name):
    """Grouping key that forgives case and plural drift
    ('Puberty blockers trial' files with 'puberty blocker trial')."""
    toks = str(name or "").casefold().split()
    return " ".join(t[:-1] if t.endswith("s") and len(t) > 3 else t
                    for t in toks)


def _bench_of(mid, members, gov_party):
    """gb = governing-party backbencher, fr = government frontbench,
    opp = everyone else. Unknown MPs count as opp-side 'other'."""
    m = members.get(str(mid))
    if not m:
        return "opp"
    if m.get("party") == gov_party:
        return "fr" if m.get("roleType") == "gov" else "gb"
    return "opp"


def _window_board(rows, members, gov_party, floor):
    """Aggregate one date-window of rows into a ranked issue list."""
    agg = {}
    for r in rows:
        k = _issue_key(r.get("issue"))
        if not k:
            continue
        a = agg.setdefault(k, {"names": {}, "tones": {}, "stances": {},
                               "bench": {"gb": [0, 0], "opp": [0, 0],
                                         "fr": [0, 0]},
                               "recs": []})
        nm = str(r.get("issue") or "").strip()
        a["names"][nm] = a["names"].get(nm, 0) + 1
        tone = r.get("tone") or ""
        stance = r.get("stance") or ""
        w = HEAT_W.get(tone, 0)
        a["tones"][tone] = a["tones"].get(tone, 0) + 1
        a["stances"][stance] = a["stances"].get(stance, 0) + 1
        b = _bench_of(r.get("mid"), members, gov_party)
        a["bench"][b][0] += 1
        a["bench"][b][1] += w
        if w >= 2 and r.get("quote"):
            m = members.get(str(r.get("mid"))) or {}
            a["recs"].append((w, r.get("said_on") or "", {
                "who": m.get("name", "an MP"),
                "party": m.get("party", ""),
                "tone": tone,
                "d": r.get("said_on") or "",
                "q": str(r.get("quote"))[:240]}))
    out = []
    for k, a in agg.items():
        n = sum(a["tones"].values())
        if n < floor:
            continue
        pts = sum(HEAT_W.get(t, 0) * c for t, c in a["tones"].items())
        recs = [r for _, _, r in
                sorted(a["recs"], key=lambda x: (-x[0], x[1]))][:HEAT_RECEIPTS]
        out.append({
            "key": k,
            "issue": max(a["names"], key=a["names"].get),
            "n": n,
            "heat": round(pts / n, 2),
            "tones": a["tones"],
            "stances": a["stances"],
            "bench": {b: v for b, v in a["bench"].items() if v[0]},
            "receipts": recs,
        })
    out.sort(key=lambda x: (-x["heat"], -x["n"]))
    return out


def _attach_trend(board, prev_rows, members, gov_party, floor):
    """Trend = heat now minus heat in the previous window of the same
    size. Needs at least half the floor in the previous window to say
    anything; otherwise the issue is marked new."""
    prev = {}
    for r in prev_rows:
        k = _issue_key(r.get("issue"))
        if not k:
            continue
        p = prev.setdefault(k, [0, 0])
        p[0] += 1
        p[1] += HEAT_W.get(r.get("tone") or "", 0)
    for it in board:
        p = prev.get(it["key"])
        if p and p[0] >= max(3, floor // 2):
            it["trend"] = round(it["heat"] - p[1] / p[0], 2)
        else:
            it["trend"] = None


def _ai_verdicts(board):
    """One small call: a one-line editorial verdict per top issue,
    grounded only in the numbers and quotes supplied. Failures are
    silent — the board simply ships without verdicts."""
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    top = board[:HEAT_VERDICTS]
    if not key or not top:
        return
    payload = [{"issue": it["issue"], "contributions": it["n"],
                "heat": it["heat"], "trend": it.get("trend"),
                "tones": it["tones"], "stances": it["stances"],
                "quotes": [r["q"] for r in it["receipts"]][:2]}
               for it in top]
    prompt = (
        "You write one-line verdicts for a parliamentary heat board. "
        "For each issue below you get its stats from the last 10 sitting "
        "days of the Commons: contribution count, heat score (0 calm to "
        "3 furious), trend vs the previous fortnight, tone mix, stance "
        "mix (relative to the government position), and sample quotes.\n"
        "Write ONE sentence per issue, under 20 words, in plain "
        "newspaper English. State only what the supplied numbers and "
        "quotes support. Never use em dashes. No hype words.\n"
        "Reply with ONLY a JSON object mapping each issue name exactly "
        "as given to its sentence. No other text.\n\n"
        + json.dumps(payload, ensure_ascii=False))
    try:
        r = requests.post(ANTHROPIC_URL, timeout=120, headers={
            "x-api-key": key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }, json={
            "model": "claude-sonnet-4-6",
            "max_tokens": 1200,
            "messages": [{"role": "user", "content": prompt}],
        })
        txt = "".join(b.get("text", "") for b in r.json().get("content", []))
        txt = txt.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        verdicts = json.loads(txt)
    except Exception as e:
        print(f"heat: verdicts skipped ({e})")
        return
    for it in board:
        v = verdicts.get(it["issue"])
        if isinstance(v, str) and v.strip():
            it["verdict"] = v.strip().replace("\u2014", ",").replace("\u2013", ",")
    print(f"heat: verdicts written for {sum(1 for i in board if i.get('verdict'))} issues")




HEAT_AREAS = ["Economy & business", "Health & care", "Defence & security",
              "Foreign affairs", "Home affairs & justice",
              "Education & children", "Energy & environment", "Transport",
              "Housing & communities", "Welfare & pensions",
              "Devolution & the Union", "Parliament & constitution"]


def _map_new_issues_to_areas(unmapped):
    """One small AI call: file new issue names under the fixed list of
    policy areas. Returns {} on any failure; unmapped issues simply sit
    out of the Policy tab until mapped."""
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not key or not unmapped:
        return {}
    prompt = ("File each parliamentary issue name under exactly one of "
              "these policy areas: " + json.dumps(HEAT_AREAS) + ".\n"
              "Issues: " + json.dumps(sorted(unmapped), ensure_ascii=False)
              + "\nReply with ONLY a JSON object mapping each issue name "
              "exactly as given to one area name exactly as given.")
    try:
        r = requests.post(ANTHROPIC_URL, timeout=120, headers={
            "x-api-key": key, "anthropic-version": "2023-06-01",
            "content-type": "application/json"},
            json={"model": "claude-sonnet-4-6", "max_tokens": 2000,
                  "messages": [{"role": "user", "content": prompt}]})
        txt = "".join(b.get("text", "") for b in r.json().get("content", []))
        txt = txt.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        out = json.loads(txt)
        return {k: v for k, v in out.items() if v in HEAT_AREAS}
    except Exception as e:
        print(f"areas: mapping call skipped ({e})")
        return {}



def _area_voices(rows, area_of, members):
    """Three characters per policy area: the workhorse (most vocal),
    the attacker (hottest when challenging the government, floor of 5
    challenging contributions), and the chief scrutineer (most asking).
    """
    def _is_chair(mid):
        m = members.get(mid) or {}
        role = (m.get("role") or "").lower()
        return (m.get("party") == "Spk" or "speaker" in role
                or "ways and means" in role)

    agg = {}
    for r in rows:
        area = area_of.get(r.get("issue"))
        mid = str(r.get("mid") or "")
        if not area or not mid or _is_chair(mid):
            continue
        a = agg.setdefault(area, {}).setdefault(
            mid, {"n": 0, "pts": 0, "ch_n": 0, "ch_pts": 0, "seek": 0})
        w = HEAT_W.get(r.get("tone") or "", 0)
        a["n"] += 1
        a["pts"] += w
        st = r.get("stance") or ""
        if st in ("opposing", "pushing further"):
            a["ch_n"] += 1
            a["ch_pts"] += w
        if st == "seeking":
            a["seek"] += 1

    def who(mid, val, note):
        m = members.get(mid) or {}
        return {"who": m.get("name", "an MP"),
                "party": m.get("party", ""), "v": val, "note": note}

    out = {}
    for area, mps in agg.items():
        v = {}
        vocal = max(mps.items(), key=lambda kv: kv[1]["n"])
        v["vocal"] = who(vocal[0], f"{vocal[1]['n']} contributions",
                         f"spoke at {vocal[1]['pts']/vocal[1]['n']:.2f}")
        attackers = [(mid, a) for mid, a in mps.items() if a["ch_n"] >= 5]
        if attackers:
            mid, a = max(attackers, key=lambda kv: kv[1]["ch_pts"]/kv[1]["ch_n"])
            v["attack"] = who(mid, f"{a['ch_pts']/a['ch_n']:.2f}",
                              f"across {a['ch_n']} challenging contributions")
        seekers = [(mid, a) for mid, a in mps.items() if a["seek"] >= 3]
        if seekers:
            mid, a = max(seekers, key=lambda kv: kv[1]["seek"])
            v["seeking"] = who(mid, f"{a['seek']} questions",
                               "genuinely asking, not attacking")
        out[area] = v
    return out


def _area_rollup(board, area_of, floor):
    """Group a window's ranked issues into policy areas. Same score,
    added up: an area's number is the average across every contribution
    made on its issues."""
    agg = {}
    for it in board:
        area = area_of.get(it["issue"])
        if not area:
            continue
        a = agg.setdefault(area, {"n": 0, "pts": 0.0, "issues": []})
        a["n"] += it["n"]
        a["pts"] += it["heat"] * it["n"]
        a["issues"].append({"issue": it["issue"], "heat": it["heat"],
                            "n": it["n"]})
    out = []
    for area, a in agg.items():
        if a["n"] < floor:
            continue
        out.append({"area": area, "n": a["n"],
                    "heat": round(a["pts"] / a["n"], 2),
                    "top": sorted(a["issues"],
                                  key=lambda x: (-x["heat"], -x["n"]))[:3]})
    out.sort(key=lambda x: (-x["heat"], -x["n"]))
    return out


def export_sentiment(members, rows):
    """Build data/sentiment.json from the permanent store:
    - per-MP monthly tone trajectory (last 6 months) with a plain
      rule-based sentence, no AI cost
    - sentiment league tables: hottest speakers, escalators, calm
      operators. Floors keep small samples off every table."""
    if not rows:
        return
    months = sorted({r["said_on"][:7] for r in rows if r.get("said_on")})[-12:]
    if not months:
        return
    mstats, counts_by_mid, challenge_by_mid = {}, {}, {}
    for r in rows:
        d = r.get("said_on") or ""
        if d[:7] not in months:
            continue
        mid = str(r.get("mid") or "")
        if not mid:
            continue
        s = mstats.setdefault(mid, {m: [0, 0] for m in months})
        s[d[:7]][0] += 1
        s[d[:7]][1] += HEAT_W.get(r.get("tone") or "", 0)
        c = counts_by_mid.setdefault(mid, {"st": {}, "tn": {}})
        st, tn = r.get("stance") or "", r.get("tone") or ""
        if st:
            c["st"][st] = c["st"].get(st, 0) + 1
        if tn:
            c["tn"][tn] = c["tn"].get(tn, 0) + 1
        # attack index raw material: challenging contributions, by month
        if st in ("opposing", "pushing further"):
            ch = challenge_by_mid.setdefault(mid, {})
            mo = ch.setdefault(d[:7], [0, 0])
            mo[0] += 1
            mo[1] += HEAT_W.get(tn, 0)

    def sentence(series):
        """series = [(month, n, heat)] for months with speech."""
        live = [(m, n, h) for m, n, h in series if n >= 3]
        if len(live) < 2:
            return ""
        recent = [h for _, _, h in live[-2:]]
        early = [h for _, _, h in live[:-2]] or recent
        r_avg = sum(recent) / len(recent)
        e_avg = sum(early) / len(early)
        mname = {"01": "January", "02": "February", "03": "March",
                 "04": "April", "05": "May", "06": "June", "07": "July",
                 "08": "August", "09": "September", "10": "October",
                 "11": "November", "12": "December"}[live[-2][0][5:7]]
        if r_avg - e_avg >= 0.25:
            return f"more heated since {mname}"
        if e_avg - r_avg >= 0.25:
            return f"calmer since {mname}"
        if r_avg >= 0.8:
            return "consistently heated"
        if r_avg <= 0.2:
            return "measured throughout"
        return "steady"

    mps_out, qual = {}, []
    q_from = (date.today() - timedelta(days=90)).isoformat()[:7]
    for mid, s in mstats.items():
        series = [(m, n, round(p / n, 2) if n else 0)
                  for m, (n, p) in sorted(s.items())]
        total = sum(n for _, n, _ in series)
        if total < 3:
            continue
        mps_out[mid] = {"t": [[m[5:], n, h] for m, n, h in series],
                        "s": sentence(series),
                        "c": counts_by_mid.get(mid, {})}
        # quarter stats for the league tables
        qn = sum(n for m, n, _ in series if m >= q_from)
        qp = sum(round(h * n) for m, n, h in series if m >= q_from)
        if qn:
            qual.append((mid, qn, qp / qn, series))

    def row(mid, val, extra=""):
        m = members.get(mid) or {}
        return {"mid": mid, "name": m.get("name", "Unknown MP"),
                "party": m.get("party", ""), "v": val, "x": extra}

    # attack index: heat across their challenging contributions only,
    # last quarter, floor of 10 challenging contributions
    atk = []
    for mid, ch in challenge_by_mid.items():
        qn = sum(v[0] for m, v in ch.items() if m >= q_from)
        qp = sum(v[1] for m, v in ch.items() if m >= q_from)
        if qn >= 10:
            atk.append((mid, qn, qp / qn))
    attack = [row(mid, round(h, 2), f"{n} challenging contributions")
              for mid, n, h in sorted(atk, key=lambda a: -a[2])[:10]]
    calm = [row(mid, round(h, 2), f"{n} contributions")
            for mid, n, h, _ in
            sorted((q for q in qual if q[1] >= 40),
                   key=lambda q: q[2])[:10]]
    esc = []
    for mid, n, h, series in qual:
        live = [(m, nn, hh) for m, nn, hh in series if nn >= 5]
        if len(live) < 3:
            continue
        delta = (sum(hh for _, _, hh in live[-2:]) / 2
                 - sum(hh for _, _, hh in live[:-2]) / len(live[:-2]))
        esc.append((mid, round(delta, 2), n))
    escalators = [row(mid, (f"+{d:.2f}" if d > 0 else f"{d:.2f}"),
                      f"{n} contributions")
                  for mid, d, n in sorted(esc, key=lambda e: -e[1])[:10]
                  if d > 0.1]

    MFULL = {"01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
             "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
             "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec"}
    span = (f"{MFULL[months[0][5:]]} {months[0][:4]} to "
            f"{MFULL[months[-1][5:]]} {months[-1][:4]}")
    save("sentiment.json", {
        "updated": date.today().isoformat(),
        "span": span,
        "months": [m[5:] for m in months],
        "mps": mps_out,
        "leagues": {"attack": attack, "escalators": escalators,
                    "calm": calm},
    })
    print(f"sentiment: {len(mps_out)} MP trajectories, "
          f"{len(attack)}/{len(escalators)}/{len(calm)} league rows")


def export_heat(members, aliases=None, rows=None):
    """Build data/heat.json: two ready-made boards (last 10 sitting days
    and last quarter) with heat, trend, chips data, receipts, verdicts."""
    if rows is None:
        rows = fetch_speech_history()
    if not rows:
        return
    # pool splintered names through the card index before ranking
    if aliases:
        for r in rows:
            r["issue"] = aliases.get(r.get("issue"), r.get("issue"))
    # governing party = the party holding the most government posts
    tally = {}
    for m in members.values():
        if m.get("roleType") == "gov":
            tally[m.get("party")] = tally.get(m.get("party"), 0) + 1
    gov_party = max(tally, key=tally.get) if tally else "Lab"

    days = sorted({r["said_on"] for r in rows if r.get("said_on")})
    fort_days = set(days[-10:])
    prev_fort_days = set(days[-20:-10])
    today = date.today()
    q_from = (today - timedelta(days=90)).isoformat()
    pq_from = (today - timedelta(days=180)).isoformat()

    fort_rows = [r for r in rows if r.get("said_on") in fort_days]
    prev_fort_rows = [r for r in rows if r.get("said_on") in prev_fort_days]
    qtr_rows = [r for r in rows if (r.get("said_on") or "") >= q_from]
    pq_rows = [r for r in rows
               if pq_from <= (r.get("said_on") or "") < q_from]

    fort = _window_board(fort_rows, members, gov_party, HEAT_FLOOR_FORT)
    _attach_trend(fort, prev_fort_rows, members, gov_party, HEAT_FLOOR_FORT)
    qtr = _window_board(qtr_rows, members, gov_party, HEAT_FLOOR_QTR)
    _attach_trend(qtr, pq_rows, members, gov_party, HEAT_FLOOR_QTR)

    # policy areas: card index on disk, AI maps anything new, rollup
    area_of = load("issue_areas.json", {})
    on_boards = {it["issue"] for it in fort} | {it["issue"] for it in qtr}
    new_map = _map_new_issues_to_areas(on_boards - set(area_of))
    if new_map:
        area_of.update(new_map)
        save("issue_areas.json", area_of)
        print(f"areas: mapped {len(new_map)} new issues")

    fort_areas = _area_rollup(fort, area_of, HEAT_FLOOR_FORT)
    qtr_areas = _area_rollup(qtr, area_of, HEAT_FLOOR_QTR)
    for areas, wrows in ((fort_areas, fort_rows), (qtr_areas, qtr_rows)):
        voices = _area_voices(wrows, area_of, members)
        for a in areas:
            a["voices"] = voices.get(a["area"], [])

    _ai_verdicts(fort)
    # quarter rows borrow the verdict where the same issue appears
    vmap = {it["key"]: it.get("verdict") for it in fort if it.get("verdict")}
    for it in qtr:
        if it["key"] in vmap and "verdict" not in it:
            it["verdict"] = vmap[it["key"]]

    for board in (fort, qtr):
        for it in board:
            it.pop("key", None)

    save("heat.json", {
        "updated": today.isoformat(),
        "gov_party": gov_party,
        "fortnight": {"label": "Last 10 sitting days",
                      "from": min(fort_days) if fort_days else "",
                      "to": max(fort_days) if fort_days else "",
                      "floor": HEAT_FLOOR_FORT, "issues": fort,
                      "areas": fort_areas},
        "quarter": {"label": "Last quarter",
                    "from": q_from, "to": days[-1] if days else "",
                    "floor": HEAT_FLOOR_QTR, "issues": qtr,
                    "areas": qtr_areas},
    })
    print(f"heat: exported {len(fort)} fortnight / {len(qtr)} quarter issues")


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

    aliases = fetch_issue_aliases()
    known_issues = fetch_recent_issue_names(aliases)
    ai_classify_debates(pending, mp_q, members, known_issues)
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

    # bake the Heat board and sentiment summaries from the
    # permanent store: one database read feeds both
    hist_rows = fetch_speech_history()
    if aliases:
        for hr in hist_rows:
            hr["issue"] = aliases.get(hr.get("issue"), hr.get("issue"))
    export_heat(members, None, hist_rows)
    export_sentiment(members, hist_rows)

    # bake the Bills tab, guarded so a wobble in Parliament's Bills API
    # can never stop the rest of the nightly run
    try:
        export_bills(members)
    except Exception as e:
        print(f"bills: export failed, keeping yesterday's file ({e})")

    save("state.json", state)
    save("votes.json", votes)
    save("q_monthly.json", q_monthly)
    save("mp_q.json", mp_q)
    print("=== done ===")


if __name__ == "__main__":
    main()
