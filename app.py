"""
app.py
======
Fresher Off-Campus Job Alert Bot.

Every day at 06:00 (Asia/Kolkata by default) this service:
  1. Crawls a set of seed pages + search-engine result pages (Crawl4AI)
     looking for off-campus fresher drives / exams / hiring challenges.
  2. Sends each crawled page's text to Gemini (with a 4-step fallback
     chain) to extract structured job data AND classify the selection
     process (direct test/assessment/interview vs. resume shortlisting).
  3. Applies a hard eligibility filter:
       - 2027 passout
       - B.E / B.Tech, CSE or IT branch
       - 0 years experience
       - NO resume shortlisting step
       - Off-campus
       - Application window currently OPEN (deadline in the future)
  4. Deduplicates against MongoDB (by company + job title + apply link).
  5. Scores + sorts the surviving jobs.
  6. Emails a structured HTML table:
       Company | Job Post | Job Requirement | Application Link | Deadline

Run:
    pip install -r requirements.txt
    python app.py

Endpoints:
    GET  /              health check
    POST /run-now        manually trigger the full pipeline (useful for testing)
    GET  /jobs            JSON list of currently-open jobs stored in Mongo

NOTE ON REALISM: automated discovery of "which off-campus drives are open
today" is inherently fuzzy — job pages disappear, sources vary in quality,
and Google's search HTML changes often, which is why this file separates
DISCOVERY (best-effort, source list in config.py) from a strict
ELIGIBILITY + SELECTION-PROCESS filter driven by an LLM, and always
verifies there's a real application link before including a row. Expect to
tune config.SEED_URLS and config.SEARCH_QUERIES over time as sources change.
"""

import asyncio
import json
import logging
import os
import re
import smtplib
import socket
import ssl
import threading
import time
from datetime import datetime, timedelta, timezone as dt_timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from urllib.parse import quote_plus

import google.generativeai as genai
import httpx
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import Flask, jsonify
from pymongo import MongoClient
from pymongo.errors import PyMongoError

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("job_alert_bot")

app = Flask(__name__)

# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------
mongo_client = MongoClient(config.MONGO_URI)
db = mongo_client[config.DB_NAME]
jobs_col = db[config.COLLECTION_JOBS]
runs_col = db[config.COLLECTION_RUNS]
rejected_col = db["rejected_jobs"]
jobs_col.create_index("dedup_key", unique=True)

# ---------------------------------------------------------------------------
# Gemini setup
# ---------------------------------------------------------------------------
if config.GEMINI_API_KEY:
    genai.configure(api_key=config.GEMINI_API_KEY)
else:
    log.warning("GEMINI_API_KEY is not set — extraction step will fail until it is.")

EXTRACTION_PROMPT_TEMPLATE = """You are a strict job-listing extraction and eligibility engine.

Read the RAW PAGE CONTENT below (crawled from a website that may list one or
more off-campus job/exam openings for freshers in India). Extract EVERY
distinct job/exam opening you can find, and for each one return the fields
below. If the page has no relevant openings, return an empty list.

Return ONLY valid JSON (no markdown fences, no commentary) matching this
exact schema:

{{
  "openings": [
    {{
      "company": "string",
      "job_post": "string (role/exam name, e.g. 'TCS NQT 2027')",
      "job_requirement": "string (1-2 line eligibility summary)",
      "apply_link": "string (direct application URL, absolute if possible)",
      "application_deadline": "string YYYY-MM-DD, or 'not specified'",
      "passout_years": ["list of graduation years this applies to, as strings"],
      "degree_types": ["list, e.g. B.Tech, B.E, M.Tech, Any Graduate"],
      "branches": ["list of eligible branches mentioned, e.g. CSE, IT, ECE, Any"],
      "min_experience_years": 0,
      "is_off_campus": true,
      "selection_process": "string describing the hiring/selection steps",
      "has_resume_shortlisting": true,
      "application_currently_open": true
    }}
  ]
}}

Rules:
- "has_resume_shortlisting" must be true if resumes/CVs/profiles are screened
  or shortlisted as a step BEFORE a test/interview. It must be false if the
  process is a DIRECT online test, coding assessment, hackathon, exam, or
  walk-in interview open to all eligible applicants without a resume-screen
  gate.
- "application_currently_open" must be false if the deadline has clearly
  passed, registration is closed, or the page says "results declared" /
  "registration closed".
- If a field is not mentioned on the page, make a reasonable inference from
  context, or use "not specified" / [] / false as appropriate — never invent
  a fake specific URL or date.
- Only include genuine job/exam openings, not generic articles about a
  company.

RAW PAGE CONTENT (may be truncated):
---
{content}
---
"""


def call_gemini_with_fallback(prompt: str) -> str | None:
    """
    Try each model in config.GEMINI_MODEL_FALLBACK_CHAIN in order.
    Within a model, retry on rate-limit (429/quota) errors with linear
    backoff before moving on to the next model in the chain. Returns the
    raw text response from the first model that succeeds, or None if every
    model (and every retry) fails.
    """
    last_error = None
    for model_name in config.GEMINI_MODEL_FALLBACK_CHAIN:
        for attempt in range(config.GEMINI_RATE_LIMIT_RETRIES):
            try:
                log.info("Gemini: trying model '%s' (attempt %d)", model_name, attempt + 1)
                model = genai.GenerativeModel(model_name)
                response = model.generate_content(
                    prompt,
                    generation_config={"response_mime_type": "application/json"},
                )
                text = (response.text or "").strip()
                if text:
                    log.info("Gemini: model '%s' succeeded", model_name)
                    return text
                raise ValueError("empty response text")
            except Exception as exc:  # noqa: BLE001 - deliberately broad for fallback loop
                last_error = exc
                msg = str(exc)
                msg_lower = msg.lower()
                is_not_found = "404" in msg or "not found" in msg_lower or "not supported" in msg_lower
                is_rate_limit = (
                    not is_not_found
                    and ("429" in msg or "quota" in msg_lower or "rate limit" in msg_lower or "resource_exhausted" in msg_lower)
                )
                if is_rate_limit and attempt < config.GEMINI_RATE_LIMIT_RETRIES - 1:
                    wait = config.GEMINI_RATE_LIMIT_BACKOFF_SECONDS * (attempt + 1)
                    log.warning(
                        "Gemini: model '%s' rate-limited, waiting %.0fs before retry: %s",
                        model_name, wait, exc,
                    )
                    time.sleep(wait)
                    continue
                log.warning("Gemini: model '%s' failed: %s", model_name, exc)
                break  # give up on this model, move to the next one in the chain
    log.error("Gemini: all models in fallback chain failed. Last error: %s", last_error)
    return None


def extract_jobs_from_content(url: str, content: str) -> list[dict]:
    """Send crawled page content to Gemini and parse structured openings."""
    if not content or len(content.strip()) < 100:
        return []

    truncated = content[:15000]  # keep prompt size sane
    prompt = EXTRACTION_PROMPT_TEMPLATE.format(content=truncated)
    raw = call_gemini_with_fallback(prompt)
    if not raw:
        return []

    try:
        cleaned = re.sub(r"^```(json)?|```$", "", raw.strip(), flags=re.MULTILINE).strip()
        data = json.loads(cleaned)
        openings = data.get("openings", [])
        for op in openings:
            op["source_url"] = url
        return openings
    except (json.JSONDecodeError, AttributeError) as exc:
        log.warning("Failed to parse Gemini JSON for %s: %s", url, exc)
        return []


# ---------------------------------------------------------------------------
# Crawling (Jina AI Reader — https://r.jina.ai/<url>)
# ---------------------------------------------------------------------------
# No browser, no OS deps: Jina renders the page server-side and hands back
# clean Markdown. We still have to be careful on the free tier though:
#   - It's a REQUESTS-PER-MINUTE limit (not a monthly quota), enforced per
#     IP (unauthenticated) or per API key. Going over it gets you a 429.
#   - The free key tier also caps CONCURRENT in-flight requests (docs cite 2
#     concurrent on free), so hammering it in parallel causes 429s even if
#     you're under the per-minute count.
#   - Occasional slow/hanging pages are normal (2s typical, but complex/JS
#     heavy pages can take much longer) — a stuck request must not be
#     allowed to block the whole run, so every request gets a hard timeout.
JINA_READER_BASE = "https://r.jina.ai/"


def build_search_urls() -> list[str]:
    """Turn config.SEARCH_QUERIES into search-engine result page URLs."""
    urls = []
    for q in config.SEARCH_QUERIES:
        urls.append(f"https://www.google.com/search?q={quote_plus(q)}&num=20")
    return urls


class _RateLimiter:
    """
    Simple async sliding-window rate limiter: allows at most `max_per_minute`
    calls to acquire() within any rolling 60s window. Callers await
    acquire() right before making the actual HTTP request.
    """

    def __init__(self, max_per_minute: int):
        self.max_per_minute = max(1, max_per_minute)
        self._timestamps: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self._lock:
                now = time.monotonic()
                # drop timestamps older than 60s
                self._timestamps = [t for t in self._timestamps if now - t < 60]
                if len(self._timestamps) < self.max_per_minute:
                    self._timestamps.append(now)
                    return
                # need to wait until the oldest timestamp falls out of the window
                sleep_for = 60 - (now - self._timestamps[0]) + 0.05
            await asyncio.sleep(max(sleep_for, 0.05))


async def _fetch_one(
    client: httpx.AsyncClient,
    url: str,
    semaphore: asyncio.Semaphore,
    rate_limiter: "_RateLimiter",
) -> tuple[str, str | None]:
    """
    Fetch a single URL through the Jina Reader proxy. Returns (url, markdown)
    on success, (url, None) on failure — never raises, so one bad URL can't
    take down the whole batch.
    """
    headers = {"Accept": "text/plain"}
    if config.JINA_API_KEY:
        headers["Authorization"] = f"Bearer {config.JINA_API_KEY}"
    # Ask Jina itself to cap how long it spends rendering server-side, on
    # top of our own client-side timeout below — belt and suspenders.
    headers["X-Timeout"] = str(config.JINA_TIMEOUT_SECONDS)

    reader_url = f"{JINA_READER_BASE}{url}"

    async with semaphore:
        for attempt in range(1, config.JINA_MAX_RETRIES + 1):
            await rate_limiter.acquire()
            try:
                resp = await asyncio.wait_for(
                    client.get(reader_url, headers=headers),
                    timeout=config.JINA_TIMEOUT_SECONDS + 5,  # hard ceiling incl. network overhead
                )
                if resp.status_code == 200:
                    return url, resp.text
                if resp.status_code == 429:
                    # Rate-limited despite our limiter (e.g. shared IP, burst
                    # from a previous run) — back off and retry.
                    wait = config.JINA_RETRY_BACKOFF_SECONDS * attempt
                    log.warning(
                        "Jina 429 for %s (attempt %d/%d), backing off %.0fs",
                        url, attempt, config.JINA_MAX_RETRIES, wait,
                    )
                    await asyncio.sleep(wait)
                    continue
                if 500 <= resp.status_code < 600:
                    # Jina-side transient error — retry with light backoff.
                    log.warning(
                        "Jina %d for %s (attempt %d/%d)",
                        resp.status_code, url, attempt, config.JINA_MAX_RETRIES,
                    )
                    await asyncio.sleep(config.JINA_RETRY_BACKOFF_SECONDS)
                    continue
                log.warning("Jina returned %d for %s, skipping", resp.status_code, url)
                return url, None
            except asyncio.TimeoutError:
                log.warning(
                    "Timeout fetching %s via Jina (attempt %d/%d)",
                    url, attempt, config.JINA_MAX_RETRIES,
                )
                # no extra sleep needed — the timeout itself already cost time
                continue
            except httpx.HTTPError as exc:
                log.warning(
                    "HTTP error fetching %s via Jina (attempt %d/%d): %s",
                    url, attempt, config.JINA_MAX_RETRIES, exc,
                )
                await asyncio.sleep(config.JINA_RETRY_BACKOFF_SECONDS)
                continue

        log.error("Giving up on %s after %d attempts", url, config.JINA_MAX_RETRIES)
        return url, None


async def crawl_all(urls: list[str]) -> dict[str, str]:
    """Crawl a list of URLs concurrently via Jina Reader, return {url: markdown}."""
    results: dict[str, str] = {}
    semaphore = asyncio.Semaphore(config.JINA_MAX_CONCURRENCY)
    rate_limiter = _RateLimiter(config.JINA_RATE_LIMIT_PER_MINUTE)

    # A single shared client reuses connections; overall timeout is handled
    # per-request above via asyncio.wait_for, so we don't need a tight
    # client-level timeout here (it would fight with our own retry logic).
    limits = httpx.Limits(max_connections=config.JINA_MAX_CONCURRENCY)
    async with httpx.AsyncClient(limits=limits, timeout=None, follow_redirects=True) as client:
        tasks = [_fetch_one(client, u, semaphore, rate_limiter) for u in urls]
        for coro in asyncio.as_completed(tasks):
            url, content = await coro
            if content:
                results[url] = content

    log.info("Jina crawl finished: %d/%d URLs succeeded", len(results), len(urls))
    return results


def run_crawler(urls: list[str]) -> dict[str, str]:
    """Sync wrapper so Flask/APScheduler (sync context) can call the async crawler."""
    return asyncio.run(crawl_all(urls))


# ---------------------------------------------------------------------------
# Hard eligibility filter (deterministic, on top of Gemini's classification)
# ---------------------------------------------------------------------------
def explain_hard_filter(op: dict) -> tuple[bool, list[str]]:
    """
    Same recall-first logic as passes_hard_filter(), but returns
    (passed, reasons) instead of just a bool, so you can see exactly why
    something was kept or dropped.
    """
    reasons = []
    try:
        if op.get("has_resume_shortlisting") is True:
            reasons.append("Has resume/CV shortlisting (bot only wants direct test/interview drives).")

        if op.get("application_currently_open") is False:
            reasons.append("Explicitly marked as not currently open (closed/upcoming).")

        if op.get("is_off_campus") is False:
            reasons.append("Explicitly marked as NOT off-campus (on-campus only).")

        raw_exp = op.get("min_experience_years", 0)
        try:
            min_exp = int(raw_exp) if raw_exp not in (None, "", "not specified") else 0
        except (ValueError, TypeError):
            min_exp = 0
        if min_exp > config.MAX_EXPERIENCE_YEARS:
            reasons.append(f"Requires {min_exp}+ years experience (bot only wants freshers).")

        years_text = " ".join(str(y) for y in _as_list(op.get("passout_years"))).lower()
        if years_text and str(config.TARGET_PASSOUT_YEAR) not in years_text:
            reasons.append(f"Passout years text ({years_text!r}) doesn't mention {config.TARGET_PASSOUT_YEAR}.")

        degrees_text = " ".join(str(d) for d in _as_list(op.get("degree_types"))).lower()
        degree_ok_keywords = [d.lower() for d in config.TARGET_DEGREE] + [
            "bachelor", "graduate", "engineering", "b.sc", "bca", "any degree", "not specified",
        ]
        degree_exclusionary_keywords = ["mba only", "phd only", "postgraduate only", "m.tech only", "mca only"]
        if degrees_text:
            explicitly_excluded = any(k in degrees_text for k in degree_exclusionary_keywords)
            plausibly_included = any(k in degrees_text for k in degree_ok_keywords)
            if explicitly_excluded and not plausibly_included:
                reasons.append(f"Degree types ({op.get('degree_types')}) explicitly exclude B.Tech/B.E.")

        branches_text = " ".join(str(b) for b in _as_list(op.get("branches"))).lower()
        if branches_text:
            has_any_branch = re.search(r"\bany\b", branches_text) is not None
            has_target_branch = any(b.lower() in branches_text for b in config.TARGET_BRANCHES)
            has_generic_cs_term = any(k in branches_text for k in ["computer", "software", "engineering"])
            if not (has_any_branch or has_target_branch or has_generic_cs_term):
                reasons.append(f"Branches listed ({op.get('branches')}) don't appear to include CSE/IT.")

        deadline = parse_deadline(op.get("application_deadline"))
        if deadline and deadline < datetime.now().date():
            reasons.append(f"Application deadline ({deadline}) has already passed.")

        return (len(reasons) == 0, reasons)
    except Exception as exc:  # noqa: BLE001
        return (True, [f"Error while evaluating filter (kept for manual review): {exc}"])


def _as_list(value) -> list:
    """Gemini sometimes returns a string instead of a list for list-typed
    fields. Normalize so downstream code doesn't crash or silently misbehave."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def passes_hard_filter(op: dict) -> bool:
    """
    Recall-first eligibility filter: the goal is to NEVER drop a genuinely
    eligible opening, even at the cost of letting through a few that turn
    out not to match. Every check below only rejects on an EXPLICIT,
    confident negative signal. Missing data, ambiguous values, unexpected
    types, or parsing errors all default to KEEPING the job rather than
    dropping it — the opposite of the previous behavior.
    """
    try:
        # --- Explicit disqualifiers only ---
        if op.get("has_resume_shortlisting") is True:
            return False

        # Only reject if Gemini explicitly said "closed" (False). Missing/
        # null/unexpected values are treated as "unknown, keep it" — the
        # deadline check below is the more reliable signal for this anyway.
        if op.get("application_currently_open") is False:
            return False

        if op.get("is_off_campus") is False:
            return False

        # Experience: only reject on a value we can confidently parse AND
        # that confidently exceeds the max. Anything unparseable ("fresher",
        # "0-1 yrs", None, etc.) is treated as 0 (fresher-friendly) instead
        # of killing the whole job.
        raw_exp = op.get("min_experience_years", 0)
        try:
            min_exp = int(raw_exp) if raw_exp not in (None, "", "not specified") else 0
        except (ValueError, TypeError):
            min_exp = 0
        if min_exp > config.MAX_EXPERIENCE_YEARS:
            return False

        # Passout year: substring match against the joined text, so ranges
        # like "2025-2027" or phrases like "up to 2027 passouts" still match
        # instead of requiring an exact "2027" list entry. Empty/missing
        # years = not specified = keep (don't assume ineligible).
        years_text = " ".join(str(y) for y in _as_list(op.get("passout_years"))).lower()
        if years_text and str(config.TARGET_PASSOUT_YEAR) not in years_text:
            return False

        # Degree: broaden beyond the exact whitelist to generic terms that
        # almost always mean "B.Tech/B.E qualifies" even if Gemini phrased
        # it differently ("Bachelor's in Engineering", "Any Graduate", etc).
        # Only reject if the text is specific AND clearly excludes us
        # (e.g. "MBA only", "PhD required", "Postgraduate only").
        degrees_text = " ".join(str(d) for d in _as_list(op.get("degree_types"))).lower()
        degree_ok_keywords = [d.lower() for d in config.TARGET_DEGREE] + [
            "bachelor", "graduate", "engineering", "b.sc", "bca", "any degree", "not specified",
        ]
        degree_exclusionary_keywords = ["mba only", "phd only", "postgraduate only", "m.tech only", "mca only"]
        if degrees_text:
            explicitly_excluded = any(k in degrees_text for k in degree_exclusionary_keywords)
            plausibly_included = any(k in degrees_text for k in degree_ok_keywords)
            if explicitly_excluded and not plausibly_included:
                return False
            # if neither excluded nor matched (unfamiliar phrasing) -> keep it

        # Branch: fix the substring false-positive ("company" containing
        # "any") with a real word-boundary check, and broaden matching.
        # Empty/unfamiliar phrasing defaults to keep, not reject.
        branches_text = " ".join(str(b) for b in _as_list(op.get("branches"))).lower()
        if branches_text:
            has_any_branch = re.search(r"\bany\b", branches_text) is not None
            has_target_branch = any(b.lower() in branches_text for b in config.TARGET_BRANCHES)
            has_generic_cs_term = any(k in branches_text for k in ["computer", "software", "engineering"])
            if not (has_any_branch or has_target_branch or has_generic_cs_term):
                return False

        # Deadline: only reject if we could confidently parse a date AND
        # it's clearly in the past. Unparseable/missing deadlines are kept.
        deadline = parse_deadline(op.get("application_deadline"))
        if deadline and deadline < datetime.now().date():
            return False

        return True
    except Exception as exc:  # noqa: BLE001
        # A parsing/schema error tells us nothing about actual eligibility —
        # default to KEEPING the job so a human can eyeball it, rather than
        # silently discarding a possibly-eligible opening.
        log.warning("Hard filter error on %r — keeping job for manual review: %s", op.get("company"), exc)
        return True


def parse_deadline(value: str | None):
    if not value or value.lower() == "not specified":
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            continue
    return None


def make_dedup_key(op: dict) -> str:
    company = (op.get("company") or "").strip().lower()
    job_post = (op.get("job_post") or "").strip().lower()
    link = (op.get("apply_link") or "").strip().lower()
    return f"{company}|{job_post}|{link}"


def score_job(op: dict) -> int:
    w = config.SCORE_WEIGHTS
    score = 0
    if op.get("has_resume_shortlisting") is False:
        score += w["no_resume_screen"]
        score += w["direct_assessment"]
    score += w["eligible_2027"]
    score += w["fresher"]
    score += w["cse_it"]

    deadline = parse_deadline(op.get("application_deadline"))
    if deadline and deadline <= datetime.now().date() + timedelta(days=5):
        score += w["deadline_soon_bonus"]
    return score


# ---------------------------------------------------------------------------
# MongoDB persistence
# ---------------------------------------------------------------------------
def upsert_job(op: dict) -> bool:
    """Insert if new, update if existing. Returns True if newly inserted."""
    key = make_dedup_key(op)
    op["dedup_key"] = key
    op["last_seen_at"] = datetime.now(dt_timezone.utc)
    op["score"] = score_job(op)

    existing = jobs_col.find_one({"dedup_key": key})
    if existing:
        jobs_col.update_one({"dedup_key": key}, {"$set": op})
        return False

    op["first_seen_at"] = datetime.now(dt_timezone.utc)
    jobs_col.insert_one(op)
    return True


def get_open_jobs() -> list[dict]:
    today = datetime.now().date().isoformat()
    docs = list(jobs_col.find({}))
    open_jobs = []
    for d in docs:
        deadline = parse_deadline(d.get("application_deadline"))
        if d.get("application_currently_open") and (deadline is None or deadline.isoformat() >= today):
            open_jobs.append(d)
    open_jobs.sort(key=lambda x: x.get("score", 0), reverse=True)
    return open_jobs


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------
def build_email_html(jobs: list[dict]) -> str:
    if not jobs:
        return "<p>No matching off-campus openings with an open application window were found today.</p>"

    rows = ""
    for j in jobs:
        rows += f"""
        <tr>
          <td style="padding:8px;border:1px solid #ddd;">{j.get('company', '')}</td>
          <td style="padding:8px;border:1px solid #ddd;">{j.get('job_post', '')}</td>
          <td style="padding:8px;border:1px solid #ddd;">{j.get('job_requirement', '')}</td>
          <td style="padding:8px;border:1px solid #ddd;">
            <a href="{j.get('apply_link', '#')}">Apply</a>
          </td>
          <td style="padding:8px;border:1px solid #ddd;">{j.get('application_deadline', 'not specified')}</td>
        </tr>"""

    return f"""
    <html><body style="font-family:Arial,sans-serif;">
      <h2>Off-Campus Fresher Openings — 2027 Batch (CSE/IT) — No Resume Shortlisting</h2>
      <p>{len(jobs)} open opportunit{'y' if len(jobs) == 1 else 'ies'} found as of {datetime.now().strftime('%d %b %Y, %I:%M %p')}.</p>
      <table style="border-collapse:collapse;width:100%;">
        <thead>
          <tr style="background:#f2f2f2;">
            <th style="padding:8px;border:1px solid #ddd;text-align:left;">Company</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left;">Job Post</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left;">Job Requirement</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left;">Application Link</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left;">Deadline</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
    </body></html>
    """


def _smtp_connect_ipv4(host: str, port: int, timeout: int = 20):
    """
    Resolve host to IPv4 only and return (family, sockaddr).

    Railway (and several other PaaS hosts) resolve smtp.gmail.com to an
    IPv6 address but don't route IPv6 traffic out of the container, which
    surfaces as `OSError: [Errno 101] Network is unreachable` even though
    the exact same code works fine locally. Forcing AF_INET sidesteps that.
    """
    addrinfo = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
    if not addrinfo:
        raise OSError(f"Could not resolve {host}:{port} over IPv4")
    return addrinfo[0]  # (family, socktype, proto, canonname, sockaddr)


def _send_via_starttls(host: str, port: int, force_ipv4: bool, msg) -> None:
    server = smtplib.SMTP(timeout=20)
    if force_ipv4:
        family, socktype, proto, _, sockaddr = _smtp_connect_ipv4(host, port)
        sock = socket.socket(family, socktype, proto)
        sock.settimeout(20)
        sock.connect(sockaddr)
        server.sock = sock
        server.file = sock.makefile("rb")
        server._host = host  # noqa: SLF001 - needed for TLS SNI/hostname checks
        code, _ = server.getreply()
        if code != 220:
            raise smtplib.SMTPConnectError(code, "Did not receive 220 greeting")
        server.ehlo()
    else:
        server.connect(host, port)
        server.ehlo()

    try:
        server.starttls(context=ssl.create_default_context())
        server.ehlo()
        server.login(config.EMAIL_ID, config.EMAIL_PASSWORD)
        server.sendmail(config.EMAIL_ID, config.RECEIVER_EMAIL_ID, msg.as_string())
    finally:
        try:
            server.quit()
        except Exception:  # noqa: BLE001
            server.close()


def _send_via_ssl(host: str, port: int, force_ipv4: bool, msg) -> None:
    context = ssl.create_default_context()
    server = smtplib.SMTP_SSL(timeout=20, context=context)
    if force_ipv4:
        family, socktype, proto, _, sockaddr = _smtp_connect_ipv4(host, port)
        raw_sock = socket.socket(family, socktype, proto)
        raw_sock.settimeout(20)
        raw_sock.connect(sockaddr)
        tls_sock = context.wrap_socket(raw_sock, server_hostname=host)
        server.sock = tls_sock
        server.file = tls_sock.makefile("rb")
        code, _ = server.getreply()
        if code != 220:
            raise smtplib.SMTPConnectError(code, "Did not receive 220 greeting")
        server.ehlo()
    else:
        server.connect(host, port)
        server.ehlo()

    try:
        server.login(config.EMAIL_ID, config.EMAIL_PASSWORD)
        server.sendmail(config.EMAIL_ID, config.RECEIVER_EMAIL_ID, msg.as_string())
    finally:
        try:
            server.quit()
        except Exception:  # noqa: BLE001
            server.close()


def send_email(html_body: str):
    if not (config.EMAIL_ID and config.EMAIL_PASSWORD and config.RECEIVER_EMAIL_ID):
        log.error("Email credentials not fully configured — skipping send.")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Off-Campus Fresher Job Alerts — {datetime.now().strftime('%d %b %Y')}"
    msg["From"] = config.EMAIL_ID
    msg["To"] = config.RECEIVER_EMAIL_ID
    msg.attach(MIMEText(html_body, "html"))

    # On Railway (and similar PaaS hosts), smtp.gmail.com can resolve to an
    # IPv6 address that the container can't route to, causing
    # "Network is unreachable" even though the identical code works fine
    # locally. We detect that case and force an IPv4 connection; locally
    # we use the normal smtplib path unchanged since it already works.
    force_ipv4 = os.getenv("SMTP_FORCE_IPV4", "").lower() in ("1", "true", "yes") or bool(
        os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_PROJECT_ID")
    )

    # (host, port, mode) attempts, in order.
    attempts = [(config.SMTP_HOST, config.SMTP_PORT, "starttls")]
    if config.SMTP_PORT != 465:
        attempts.append((config.SMTP_HOST, 465, "ssl"))

    last_exc = None
    for host, port, mode in attempts:
        # Try the "native" mode first (force_ipv4 as detected), then the
        # opposite as a fallback in case detection was wrong for this host.
        for ipv4_flag in (force_ipv4, not force_ipv4):
            try:
                if mode == "ssl":
                    _send_via_ssl(host, port, ipv4_flag, msg)
                else:
                    _send_via_starttls(host, port, ipv4_flag, msg)
                log.info(
                    "Email sent to %s via %s:%s (%s, force_ipv4=%s)",
                    config.RECEIVER_EMAIL_ID, host, port, mode, ipv4_flag,
                )
                return
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                log.warning(
                    "SMTP attempt via %s:%s (%s, force_ipv4=%s) failed: %s",
                    host, port, mode, ipv4_flag, exc,
                )
                continue

    log.error("Failed to send email after all attempts: %s", last_exc)


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------
def run_pipeline():
    log.info("=== Pipeline run started ===")
    run_doc = {"started_at": datetime.now(dt_timezone.utc), "status": "running"}
    run_id = runs_col.insert_one(run_doc).inserted_id

    try:
        urls = config.SEED_URLS + build_search_urls()
        log.info("Crawling %d URLs", len(urls))
        pages = run_crawler(urls)
        log.info("Crawled %d/%d URLs successfully", len(pages), len(urls))

        all_openings = []
        for url, content in pages.items():
            openings = extract_jobs_from_content(url, content)
            all_openings.extend(openings)
            time.sleep(config.GEMINI_CALL_DELAY_SECONDS)  # stay under free-tier RPM

        log.info("Extracted %d raw openings before filtering", len(all_openings))

        eligible = []
        rejected = []
        for op in all_openings:
            passed, reasons = explain_hard_filter(op)
            if passed:
                eligible.append(op)
            else:
                rejected.append({
                    "company": op.get("company"),
                    "job_post": op.get("job_post"),
                    "source_url": op.get("source_url"),
                    "reasons": reasons,
                })
        log.info("%d openings passed the hard eligibility + selection-process filter", len(eligible))
        if rejected:
            log.info("%d openings rejected — see 'rejected_jobs' collection or /rejected endpoint for reasons", len(rejected))
            for r in rejected:
                log.info("  REJECTED: %s | %s -> %s", r.get("company"), r.get("job_post"), "; ".join(r["reasons"]))

        new_count = 0
        for op in eligible:
            if upsert_job(op):
                new_count += 1

        if rejected:
            for r in rejected:
                r["run_id"] = run_id
                r["rejected_at"] = datetime.now(dt_timezone.utc)
            rejected_col.insert_many(rejected)

        open_jobs = get_open_jobs()
        html = build_email_html(open_jobs)
        send_email(html)

        runs_col.update_one(
            {"_id": run_id},
            {"$set": {
                "status": "success",
                "finished_at": datetime.now(dt_timezone.utc),
                "urls_crawled": len(pages),
                "raw_openings": len(all_openings),
                "eligible_openings": len(eligible),
                "new_jobs": new_count,
                "emailed_jobs": len(open_jobs),
            }},
        )
        log.info("=== Pipeline run finished: %d emailed, %d new ===", len(open_jobs), new_count)
    except Exception as exc:  # noqa: BLE001
        log.exception("Pipeline run failed")
        runs_col.update_one(
            {"_id": run_id},
            {"$set": {"status": "failed", "finished_at": datetime.now(dt_timezone.utc), "error": str(exc)}},
        )


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------
scheduler = BackgroundScheduler(timezone=config.TIMEZONE)
scheduler.add_job(
    run_pipeline,
    # IMPORTANT: CronTrigger resolves its own timezone independently of the
    # scheduler's `timezone=` kwarg — without passing timezone explicitly here,
    # it silently falls back to the host machine's local tz (UTC on Railway),
    # so the job would fire 5h30m early/late relative to the intended IST time.
    trigger=CronTrigger(
        hour=config.SEND_HOUR,
        minute=config.SEND_MINUTE,
        timezone=config.TIMEZONE,
    ),
    id="daily_job_alert",
    replace_existing=True,
)


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------
@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "scheduled_time": f"{config.SEND_HOUR:02d}:{config.SEND_MINUTE:02d} {config.TIMEZONE}",
        "gemini_fallback_chain": config.GEMINI_MODEL_FALLBACK_CHAIN,
    })


@app.route("/run-now", methods=["POST", "GET"])
def run_now():
    thread = threading.Thread(target=run_pipeline, daemon=True)
    thread.start()
    return jsonify({"status": "pipeline started in background, check logs / GET /jobs"})


@app.route("/jobs", methods=["GET"])
def list_jobs():
    jobs = get_open_jobs()
    for j in jobs:
        j["_id"] = str(j["_id"])
    return jsonify({"count": len(jobs), "jobs": jobs})


@app.route("/rejected", methods=["GET"])
def list_rejected():
    """Most recent run's rejected openings, with the reason(s) each was excluded."""
    latest_run = runs_col.find_one({"status": "success"}, sort=[("finished_at", -1)])
    if not latest_run:
        return jsonify({"count": 0, "rejected": [], "note": "No completed run yet."})
    docs = list(rejected_col.find({"run_id": latest_run["_id"]}))
    for d in docs:
        d["_id"] = str(d["_id"])
        d["run_id"] = str(d["run_id"])
    return jsonify({"count": len(docs), "run_id": str(latest_run["_id"]), "rejected": docs})


if __name__ == "__main__":
    scheduler.start()
    job = scheduler.get_job("daily_job_alert")
    log.info(
        "Scheduler started. Daily run at %02d:%02d %s (next run: %s)",
        config.SEND_HOUR,
        config.SEND_MINUTE,
        config.TIMEZONE,
        job.next_run_time if job else "unknown",
    )
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)