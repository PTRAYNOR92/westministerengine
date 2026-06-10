# SETUP GUIDE — Westminster Engine

Total time: about 30 minutes of clicking, once. After that the system runs itself every night.
You never need to touch code. If anything ever shows an error, copy the red text and paste it
to Claude — you'll get back a fixed file and instructions for where to paste it.

---

## PART A — Put the engine on GitHub (10 minutes)

1. Go to **github.com** and click **Sign up** (it's free). Choose any username — it will
   appear in your site's temporary address, so keep it sensible.

2. Once signed in, click the **+** icon (top right) → **New repository**.

3. Fill in:
   - Repository name: `westminster-engine` (you can rename later)
   - Set it to **Public**
   - Leave every checkbox UNTICKED
   - Click **Create repository**

4. On the next page, click the link that says **uploading an existing file**.

5. Unzip the folder Claude gave you on your computer. Open the unzipped folder so you can
   see: `index.html`, `README.md`, `SETUP.md`, and the folders `pipeline` and `data`.

6. Select ALL of those and drag them into the GitHub upload box. Wait for every file to
   show a green tick, then click **Commit changes**.

7. **The hidden folder — important.** The folder `.github` (with a dot at the front) holds
   the automation and sometimes doesn't drag in because your computer hides dot-folders.
   Check: on your repository page, can you see a `.github` folder in the file list?
   - If YES: skip to Part B.
   - If NO: click **Add file → Create new file**. In the filename box type exactly:
     `.github/workflows/nightly.yml` (GitHub turns the slashes into folders as you type).
     Open the `nightly.yml` file from the zip in any text editor (Notepad is fine),
     copy everything, paste it into the big box on GitHub, click **Commit changes**.
     Repeat for `.github/workflows/backfill.yml`.

---

## PART B — Give the robot permission to save its work (1 minute)

8. In your repository, click **Settings** (top menu) → **Actions** (left sidebar) →
   **General**.

9. Scroll to **Workflow permissions**. Select **Read and write permissions**.
   Click **Save**. (Without this, the robot can compute but can't store the results.)

---

## PART C — Run the one-time backfill (5 minutes of clicking, then it works alone)

10. Click the **Actions** tab (top menu). If a button asks you to enable workflows,
    click it.

11. In the left sidebar click **Backfill (run once, first)**.

12. Click the grey **Run workflow** dropdown (right side) → green **Run workflow** button.

13. A new run appears with an amber dot. This is the robot reading two years of
    parliamentary history — every division and every written question since July 2024.
    **It can take 1–3 hours.** Close the tab and have your evening; it doesn't need you.

14. When you come back, the dot should be a green tick. (Red cross? Click into it, click
    the step with the red cross, copy the error text, paste to Claude.)

---

## PART D — Put the site on the internet with Vercel (5 minutes)

15. Go to **vercel.com** → **Sign up** → **Continue with GitHub**. Authorise it.

16. Click **Add New… → Project**. You'll see `westminster-engine` in the list →
    click **Import**.

17. Change nothing on the next screen (Framework Preset: **Other** is correct).
    Click **Deploy**.

18. About a minute later: confetti, and a link like
    `westminster-engine.vercel.app`. Click it. That's your site, live, with real data.

From now on: every night at ~3am the robot updates the data, and Vercel republishes
the site automatically. Your ongoing workload is zero.

---

## PART E — Domain (whenever you're ready, 10 minutes)

19. Buy a domain anywhere (Namecheap, GoDaddy, Cloudflare — ~£10/year).

20. In Vercel: your project → **Settings → Domains** → type your domain → **Add**.
    Vercel shows you exactly two settings to paste into your domain provider's DNS page,
    and checks them for you. Green tick = done.

---

## Troubleshooting cheat-sheet

- **Site shows "SAMPLE DATA" banner** → the backfill hasn't run or hasn't finished (Part C).
- **A nightly run failed** → usually Parliament's API having a moment; the site keeps
  serving yesterday's data, and the next night normally self-heals. Two red crosses in a
  row → copy the error to Claude.
- **Want to run an update right now?** Actions → "Nightly data update" → Run workflow.
- **Anything else** → copy the error, paste to Claude, get a fix back.
