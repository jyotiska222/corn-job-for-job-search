# Fresher Off-Campus Job Alert Bot

## Setup

```bash
pip install -r requirements.txt
```

No browser install needed — crawling goes through the [Jina AI Reader](https://jina.ai/reader/) API (`r.jina.ai`), which renders pages server-side and returns clean Markdown, so there's no Chromium/Playwright/OS dependency at all.

Fill in `.env`:
- `MONGO_URI`, `DB_NAME` — your MongoDB Atlas connection.
- `EMAIL_ID` / `EMAIL_PASSWORD` — a Gmail **App Password** (not your login password). Generate at https://myaccount.google.com/apppasswords.
- `GEMINI_API_KEY` — **required**, get one free at https://aistudio.google.com/apikey.
- `JINA_API_KEY` — optional, get one free at https://jina.ai/reader/ (no card required). Without it, crawling still works but is rate-limited more tightly (20 req/min vs 500 req/min with a key). See "Jina AI Reader" section below.
- `PRIMARY_MODEL` / `SECONDARY_MODELS` / `TERTIARY_MODELS` — the fallback chain.
  ⚠️ "Gemini 3.8/3.7/3.6 Flash" (as given) are not confirmed real model IDs as of
  this writing. Check https://ai.google.dev/gemini-api/docs/models for the
  current valid model names (e.g. `gemini-2.0-flash`, `gemini-1.5-flash`) and
  put the real ones here — an invalid name just fails that rung and the code
  automatically falls through to the next model.

Run:
```bash
python app.py
```

## Endpoints
- `GET /` — health check, shows the scheduled time and active model fallback chain.
- `GET|POST /run-now` — manually triggers the full pipeline immediately (don't wait for 6 AM to test).
- `GET /jobs` — JSON of all currently-open jobs stored in MongoDB.

## How it decides "no resume shortlisting"
Each crawled page is sent to Gemini with a strict extraction prompt that
classifies `has_resume_shortlisting` (true/false) based on the described
selection process. Only listings where this is `false` AND
`application_currently_open` is `true` survive the hard filter in
`passes_hard_filter()` in `app.py`.

## Jina AI Reader (crawler)
Every seed/search URL is fetched via `https://r.jina.ai/<url>`, which renders
the page (including JS-heavy career portals) and returns Markdown ready for
the Gemini extraction step. This is free and has no monthly quota — it's
rate-limited **per minute**, not per month/year, so running once a day
indefinitely is fine. To be gentle with the free tier, `app.py`:
- Enforces its own client-side requests-per-minute ceiling
  (`config.JINA_RATE_LIMIT_PER_MINUTE`) so it never even attempts to exceed
  Jina's limit.
- Caps concurrent in-flight requests (`config.JINA_MAX_CONCURRENCY`, default
  2) since the free tier also limits concurrency, not just RPM.
- Applies a hard per-request timeout (`config.JINA_TIMEOUT_SECONDS`) so one
  slow/stuck page can't stall the whole crawl.
- Retries 429s and 5xxs with linear backoff (`config.JINA_MAX_RETRIES`,
  `config.JINA_RETRY_BACKOFF_SECONDS`) before giving up on a single URL —
  a failed URL is skipped, not fatal to the run.

Set `JINA_API_KEY` in `.env` to raise the ceiling from 20 req/min to 500
req/min (per Jina's published Reader API limits — still free); leave it
blank to run unauthenticated.

## Known limitations (please read before relying on this for real deadlines)
1. **Discovery quality depends on `config.SEED_URLS` / `SEARCH_QUERIES`.**
   Scraping raw Google search result HTML is fragile and may get blocked or
   return little; treat the search-query URLs as best-effort and lean on the
   seed URLs (official pages, aggregator sites) as the more reliable source.
   Add more seed URLs as you find good aggregator sites.
2. **LLMs can misclassify.** Always sanity-check a listing on the official
   apply link before submitting real personal information — this bot is a
   discovery/filter aid, not a guarantee.
3. **The Gemini model names in `.env` are placeholders** — verify against
   Google's current model list before deploying.
4. The pipeline dedupes by `company|job_post|apply_link`, so the same drive
   won't be re-emailed as "new" every day, but it **will** keep appearing in
   the daily digest for as long as `application_currently_open` stays true.
