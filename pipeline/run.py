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


def fetch_roles(members, prev):
    """Current government/opposition post per MP, from the Members API.

    One call per MP; failures fall back to the previous run's value so a
    flaky night never blanks the site.
    """
    print("Fetching roles…")
    fails = 0
    for mid, m in members.items():
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


def fetch_spoken(members, prev):
    """Count of spoken Hansard contributions this parliament, per MP.

    Uses the Hansard search API's result count (take=1 keeps it cheap).
    Failures fall back to the previous run's value.
    """
    print("Fetching spoken contributions…")
    fails = 0
    for mid, m in members.items():
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
        if len(ex) < 5:
            continue  # too few questions to theme honestly
        sig = hashlib.md5("|".join(e["t"] for e in ex)
                          .encode("utf-8")).hexdigest()[:12]
        c = cache.get(mid)
        if c and c.get("sig") == sig and c.get("themes"):
            m["themes"], m["approach"] = c["themes"], c.get("approach", {})
            m["themeN"] = c.get("n", len(ex))
        else:
            todo.append((m, ex, sig))

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
                    "questions": [e["t"] for e in ex]}
                   for m, ex, _ in batch]
        prompt = (
            "You are analysing UK MPs' written parliamentary questions for a "
            "data site. For each MP below, using ONLY their listed questions:\n"
            "1. themes: their top 1-3 subjects (most prominent first). Each "
            'has "subject" (2-5 plain words), "stance" (max 14 words on what '
            "they are doing about it — e.g. 'pressing the government over "
            "delays to compensation payments', 'seeking detail on funding "
            "allocations'; describe conduct, never characterise the person), "
            'and "eg" (the 0-based index of one listed question that best '
            "shows the theme).\n"
            '2. approach: count EVERY listed question into exactly one of: '
            '"pressing" (demanding action, challenging, chasing failures), '
            '"probing" (seeking information or detail), '
            '"constituency" (local casework framing), '
            '"supportive" (inviting good news, friendly prompts).\n'
            "Ground everything in the question texts. Respond with ONLY a "
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
        for m, ex, sig in batch:
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
                if isinstance(idx, int) and 0 <= idx < len(ex):
                    th["example"] = ex[idx]["t"]
                themes.append(th)
            if not themes:
                continue
            approach = {k: int(v) for k, v in (g.get("approach") or {}).items()
                        if k in ("pressing", "probing", "constituency",
                                 "supportive") and isinstance(v, (int, float))}
            m["themes"], m["approach"], m["themeN"] = themes, approach, len(ex)
            cache[str(m["id"])] = {"sig": sig, "themes": themes,
                                   "approach": approach, "n": len(ex),
                                   "made": date.today().isoformat()}
            done += 1
    save("mp_glosses.json", cache)
    print(f"MP themes: {done}/{len(todo)} generated this run")


# ---------------------------------------------------------------- compute
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


def compute(members, votes, q_monthly, mp_q):
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
            "spark": [q["months"].get(mo, 0) for mo in months],
            "topics": [[b, round(100 * v / bsum)] for b, v in bodies],
        })
    mps_out.sort(key=lambda m: m["name"])
    ai_mp_themes(mps_out, mp_q)

    # topic momentum: last 90 days vs previous 90, by question heading
    today = date.today()
    cur_months = {(today - timedelta(days=i * 30)).isoformat()[:7] for i in range(3)}
    prev_months = {(today - timedelta(days=(i + 3) * 30)).isoformat()[:7] for i in range(3)}
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
    for heading, a in agg.items():
        if a["cur"] < 12:
            continue
        growth = round(100 * (a["cur"] - a["prev"]) / a["prev"]) if a["prev"] >= 5 else None
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
            "cur": a["cur"], "prev": a["prev"], "growth": growth,
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
        "presence": league(lambda m: m["participation"]),
    }

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
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["backfill", "nightly"], default="nightly")
    mode = ap.parse_args().mode
    print(f"=== Westminster Engine · {mode} · {datetime.utcnow().isoformat()}Z ===")

    state = load("state.json", {})
    votes = load("votes.json", {})
    q_monthly = load("q_monthly.json", {})
    mp_q = load("mp_q.json", {})

    if mode == "backfill":
        state, votes, q_monthly, mp_q = {}, {}, {}, {}

    members = fetch_members()
    if not members:
        print("FATAL: could not fetch MPs — aborting without touching data.")
        sys.exit(1)

    prev = {str(m.get("id")): m for m in load("mps.json", [])
            if isinstance(m, dict)}
    fetch_roles(members, prev)
    fetch_spoken(members, prev)

    fetch_divisions(state, votes)
    fetch_questions(state, q_monthly, mp_q, mode)

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
        if "ex" in mq:
            mq["ex"] = sorted(
                (e for e in mq["ex"]
                 if isinstance(e, dict) and e.get("d", "") >= theme_floor),
                key=lambda e: e["d"])[-THEME_KEEP:]
            if not mq["ex"]:
                del mq["ex"]

    compute(members, votes, q_monthly, mp_q)

    save("state.json", state)
    save("votes.json", votes)
    save("q_monthly.json", q_monthly)
    save("mp_q.json", mp_q)
    print("=== done ===")


if __name__ == "__main__":
    main()
