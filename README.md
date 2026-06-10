# Westminster Engine — phase one

Performance data for Parliament: every division, every written question, all 650 MPs —
computed nightly from the official record via the UK Parliament open APIs. No opinions,
no database servers, no running costs.

**Setup:** read `SETUP.md` — it's a numbered click-by-click guide, no coding required.

## What's in here
- `index.html` — the public site (static; Vercel serves it as-is)
- `pipeline/run.py` — the data engine (members, divisions, written questions → stats)
- `.github/workflows/backfill.yml` — one-time historical load since 4 July 2024
- `.github/workflows/nightly.yml` — the 3am robot: fetch yesterday, append, recompute, publish
- `data/` — the engine's memory and the site's data (starts as samples; the backfill replaces them)

## Phase plan
- **Phase one (this repo):** pure counts — rebellion/independence, participation, question
  volumes and subjects, topic momentum, league tables.
- **Phase two (after a clean week of nightly runs):** the stance engine — tone classification
  of questions and debate speeches, friendly fire, silence signals, template-cluster
  detection, amendment co-signature networks, bill-stage tracking.

## Methodology notes
A rebellion = voting against the majority of the MP's own party in a division where that
party voted cohesively (≥60% one way, ≥10 voting). Free votes largely self-exclude by that
definition. The Speaker, Deputy Speakers and Sinn Féin do not vote; read their rows accordingly.

Contains parliamentary information licensed under the Open Parliament Licence v3.0.
