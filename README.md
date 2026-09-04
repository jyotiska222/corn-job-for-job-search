# Fresher Off-Campus Job Alert Bot

## Setup

```bash
pip install -r requirements.txt
python -m playwright install --with-deps chromium   # required by Crawl4AI
```

Fill in `.env`:
- `MONGO_URI`, `DB_NAME` — your MongoDB Atlas connection.
- `EMAIL_ID` / `EMAIL_PASSWORD` — a Gmail **App Password** (not your login password). Generate at https://myaccount.google.com/apppasswords.
- `GEMINI_API_KEY` — **required**, get one free at https://aistudio.google.com/apikey.
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
4. Crawl4AI requires a Chromium browser install (`playwright install`) the
   first time you set this up.
5. The pipeline dedupes by `company|job_post|apply_link`, so the same drive
   won't be re-emailed as "new" every day, but it **will** keep appearing in
   the daily digest for as long as `application_currently_open` stays true.
