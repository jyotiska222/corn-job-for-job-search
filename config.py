"""
config.py
---------
Central configuration for the Fresher Off-Campus Job Alert Bot.
Loads secrets from .env and defines static config: search queries,
seed URLs, eligibility rules, and the Gemini fallback chain.
"""

import os
from dotenv import load_dotenv

load_dotenv()


def _clean_env_value(raw: str) -> str:
    """
    python-dotenv only strips inline '# comment' suffixes when there's a
    real value before the '#'. A line like `KEY=      # comment` (blank
    value, comment only) is NOT stripped by dotenv and the comment text
    itself becomes the value — silently corrupting the setting. Strip any
    unquoted trailing '#...' comment ourselves as a defensive fallback.
    """
    if "#" in raw:
        raw = raw.split("#", 1)[0]
    return raw.strip()


def _getenv_int(name: str, default: int) -> int:
    """Like os.getenv but treats an unset OR blank/comment-only value as
    'use the default' — a bare `KEY=` (or `KEY=   # comment`) left behind
    after editing .env should not crash startup with int('')."""
    raw = os.getenv(name)
    if raw is None:
        return default
    cleaned = _clean_env_value(raw)
    if not cleaned:
        return default
    return int(cleaned)


def _getenv_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    cleaned = _clean_env_value(raw)
    if not cleaned:
        return default
    return float(cleaned)


# ---------------------------------------------------------------------------
# Secrets / environment
# ---------------------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "job_alert_bot")

EMAIL_ID = os.getenv("EMAIL_ID")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
RECEIVER_EMAIL_ID = os.getenv("RECEIVER_EMAIL_ID")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = _getenv_int("SMTP_PORT", 587)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")  # required for extraction/classification

# Fallback chain of Gemini models, in order of preference. These must be the
# exact API model ID (lowercase, hyphenated), NOT the display name shown in
# Google's docs/UI — passing the display name causes a 400 "unexpected model
# name format" error on every call. Defaults below are confirmed-valid,
# free-tier-eligible model IDs as of the 2026-09 rate-limit dashboard
# (gemini-3.1-flash-lite: 15 RPM / 250K TPM / 500 RPD). Override via .env if
# your account has access to something else. Get current valid IDs from
# https://ai.google.dev/gemini-api/docs/models
PRIMARY_MODEL = os.getenv("PRIMARY_MODEL", "gemini-3.1-flash-lite")
SECONDARY_MODELS = [m.strip() for m in os.getenv("SECONDARY_MODELS", "gemini-2.5-flash-lite").split(",") if m.strip()]
TERTIARY_MODELS = [m.strip() for m in os.getenv("TERTIARY_MODELS", "gemini-3.5-flash-lite").split(",") if m.strip()]

# Full ordered fallback list, deduplicated while preserving order.
_chain = [PRIMARY_MODEL] + SECONDARY_MODELS + TERTIARY_MODELS + [PRIMARY_MODEL]
GEMINI_MODEL_FALLBACK_CHAIN = list(dict.fromkeys([m for m in _chain if m]))

# Seconds to sleep between Gemini extraction calls, to stay under free-tier
# RPM limits (gemini-3.1-flash-lite allows 15 RPM -> need >=4s between calls).
GEMINI_CALL_DELAY_SECONDS = _getenv_float("GEMINI_CALL_DELAY_SECONDS", 4.5)
# Retry attempts per model on rate-limit (429) errors, with linear backoff.
GEMINI_RATE_LIMIT_RETRIES = _getenv_int("GEMINI_RATE_LIMIT_RETRIES", 3)
GEMINI_RATE_LIMIT_BACKOFF_SECONDS = _getenv_float("GEMINI_RATE_LIMIT_BACKOFF_SECONDS", 20)

# ---------------------------------------------------------------------------
# Scheduling
# ---------------------------------------------------------------------------
TIMEZONE = os.getenv("TIMEZONE", "Asia/Kolkata")
SEND_HOUR = _getenv_int("SEND_HOUR", 6)
SEND_MINUTE = _getenv_int("SEND_MINUTE", 0)

# ---------------------------------------------------------------------------
# Eligibility rules (hard filter)
# ---------------------------------------------------------------------------
TARGET_PASSOUT_YEAR = 2027
TARGET_BRANCHES = ["CSE", "IT", "Computer Science", "Information Technology"]
TARGET_DEGREE = ["B.E", "B.Tech", "BE", "BTech"]
MAX_EXPERIENCE_YEARS = 0

# Selection-process keywords that DISQUALIFY a listing (resume shortlisting present)
DISQUALIFYING_KEYWORDS = [
    "resume shortlisting", "resume shortlist", "cv shortlisting",
    "profile shortlisting", "shortlisted based on resume",
    "shortlisting based on academic", "shortlisting criteria: resume",
]

# Selection-process keywords that QUALIFY a listing (direct test/assessment/interview)
QUALIFYING_KEYWORDS = [
    "online test", "nqt", "national qualifier test", "coding test",
    "aptitude test", "direct interview", "walk-in", "hackathon",
    "coding challenge", "codevita", "assessment", "online assessment",
    "direct exam", "qualifier exam",
]

# ---------------------------------------------------------------------------
# Discovery sources
# ---------------------------------------------------------------------------
# Fixed seed pages, grouped by company. These are stable "hub" pages
# (official careers portals + off-campus aggregators) — individual per-drive
# application links change constantly and expire, so we crawl the hub page
# each run and let extraction find whatever drive is currently listed there,
# rather than hardcoding a link that will go stale.
# ---------------------------------------------------------------------------
SEED_URLS = [
    # --- TCS ---
    "https://nextstep.tcs.com/campus/#/registration",   # TCS NQT Ninja/Digital/Prime off-campus
    "https://www.tcs.com/careers/india/tcs-nqt",
    "https://www.tcscodevita.com/",                       # TCS CodeVita

    # --- Cognizant ---
    "https://careers.cognizant.com/india-en/pathways-and-programs/genc-program/",  # GenC/GenC Next/GenC Pro

    # --- Capgemini ---
    "https://www.capgemini.com/in-en/careers/job-search/?jobcategory=Fresher",     # Capgemini Analyst

    # --- LTIMindtree ---
    "https://careers.ltimindtree.com/students",           # LTIMindtree GET

    # --- Infosys ---
    "https://www.infosys.com/careers/apply.html",          # Infosys SE/DSE/SP
    "https://career.infosys.com/joblist",

    # --- Wipro ---
    "https://careers.wipro.com/careers-home/",             # Wipro Elite/Turbo NLTH

    # --- HCLTech ---
    "https://www.hcltech.com/careers/campus",              # HCLTech GET

    # --- Tech Mahindra ---
    "https://careers.techmahindra.com/",

    # --- Accenture ---
    "https://www.accenture.com/in-en/careers/jobsearch?jk=entry%20level",  # Accenture ASE

    # --- IBM ---
    "https://www.ibm.com/in-en/careers/early-professionals", # IBM Associate System Engineer

    # --- Mphasis ---
    "https://careers.mphasis.com/",

    # --- Hexaware ---
    "https://careers.hexaware.com/",

    # --- Deloitte ---
    "https://www2.deloitte.com/in/en/careers/careers-in-india/students.html",  # Deloitte USI

    # --- Amazon ---
    "https://www.amazon.jobs/en/teams/university-graduates",  # Amazon SDE (new grad)

    # --- Microsoft ---
    "https://careers.microsoft.com/students/us/en/c/software-engineering-jobs",  # SWE New Grad

    # --- Samsung ---
    "https://www.samsung.com/in/careers/job-openings/",     # Samsung R&D Institute India

    # --- Goldman Sachs ---
    "https://higher.gs.com/results?SCHOOL_GRAD_YEAR=2027",  # New Analyst program

    # --- Zoho ---
    "https://careers.zoho.com/jobs/Careers",

    # --- Freshworks ---
    "https://www.freshworks.com/company/careers/",

    # --- Razorpay ---
    "https://razorpay.com/jobs/",

    # --- General off-campus aggregators (catch drives not yet on official pages) ---
    "https://www.freshersnow.com/off-campus-drive/",
    "https://www.freejobalert.com/off-campus-drives/",
    "https://www.hirist.tech/off-campus-jobs",
    "https://www.wisdomjobs.com/off-campus-drives/",
    "https://www.freshersworld.com/off-campus-jobs",
    "https://www.linkedin.com/jobs/search/?keywords=off%20campus%20fresher%202027",
]

# Search-engine query variations used for broader discovery via Crawl4AI.
# These are turned into search-result URLs at runtime (see app.py: build_search_urls).
# Grouped by company/exam so it's easy to add/remove one without hunting
# through a flat list.
SEARCH_QUERIES = [
    # Generic
    "off campus drive 2027 batch CSE IT no resume shortlisting",
    "direct online test off campus drive 2027 CSE IT",
    "fresher off campus 2027 batch coding assessment no interview shortlisting",
    "off campus recruitment drive 2027 B.Tech CSE IT apply online",

    # TCS
    "TCS NQT 2027 batch off campus registration",
    "TCS CodeVita 2026 registration open",
    "TCS Ninja Digital Prime off campus 2027 batch",

    # Cognizant
    "Cognizant GenC GenC Next GenC Pro off campus 2027 registration",

    # Capgemini
    "Capgemini Analyst off campus drive 2027 batch registration",

    # LTIMindtree
    "LTIMindtree GET off campus 2027 batch hiring",

    # Infosys
    "Infosys Specialist Programmer Digital Specialist Engineer off campus 2027",
    "Infosys SP DSE off campus registration 2027 batch",

    # Wipro
    "Wipro Elite NLTH Wipro Turbo off campus 2027 batch registration",

    # HCLTech
    "HCLTech Graduate Engineer Trainee GET off campus 2027 batch",

    # Tech Mahindra
    "Tech Mahindra off campus drive 2027 batch fresher hiring",

    # Accenture
    "Accenture ASE Associate Software Engineer off campus 2027 batch",

    # IBM
    "IBM Associate System Engineer off campus 2027 batch India",

    # Mphasis
    "Mphasis off campus drive 2027 batch fresher hiring registration",

    # Hexaware
    "Hexaware off campus drive 2027 batch fresher hiring",

    # Deloitte
    "Deloitte USI off campus drive 2027 batch fresher hiring",

    # Amazon
    "Amazon SDE new grad 2027 batch off campus India registration",

    # Microsoft
    "Microsoft SWE new grad 2027 batch India off campus registration",

    # Samsung
    "Samsung R&D Institute India off campus 2027 batch hiring",

    # Goldman Sachs
    "Goldman Sachs new analyst program India 2027 batch off campus",

    # Zoho
    "Zoho off campus drive 2027 batch fresher hiring test",

    # Freshworks
    "Freshworks off campus drive 2027 batch fresher hiring",

    # Razorpay
    "Razorpay off campus drive 2027 batch fresher hiring",

    # National qualifier / assessment style exams generically
    "national qualifier test 2027 batch registration",

    "assessment test 2027 batch fresher hiring",

    "online assessment test 2027 batch",

    "fresher assessment test 2027 batch ",

    "freshears walk in drive 2027 batch in Kolkata",

    "freshears software engineer job for 2027 batch",
]

# ---------------------------------------------------------------------------
# Pipeline tuning
# ---------------------------------------------------------------------------
# NOTE: with a free-tier Gemini key (500 requests/day on gemini-3.1-flash-lite),
# crawling ~55 seed URLs + ~28 search queries can easily produce more pages
# than your daily quota can extract from. Keep these conservative, or split
# SEED_URLS / SEARCH_QUERIES across multiple scheduled runs if you need
# broader coverage.
MAX_PAGES_PER_QUERY = 3

# ---------------------------------------------------------------------------
# Jina AI Reader (crawler) — https://r.jina.ai/
# ---------------------------------------------------------------------------
# Optional: set JINA_API_KEY in .env for a higher free-tier rate limit
# (100 RPM w/ key vs ~20 RPM unauthenticated, per IP). Get one free at
# https://jina.ai/reader/ — no card required. Leave blank to run
# unauthenticated (fine for this bot's ~80 URLs/day workload, just slower).
JINA_API_KEY = _clean_env_value(os.getenv("JINA_API_KEY", ""))

# Requests-per-minute ceiling our own rate limiter enforces client-side, so
# we never even attempt to exceed Jina's limit (avoids wasted 429 round-trips).
# Per Jina's published Reader API limits: 20 RPM without a key, 500 RPM with
# a free API key. Kept slightly under the documented max to leave headroom
# for other traffic sharing the same IP/key.
JINA_RATE_LIMIT_PER_MINUTE = _getenv_int("JINA_RATE_LIMIT_PER_MINUTE", 450 if JINA_API_KEY else 18)

# Max simultaneous in-flight requests. Even with 500 RPM, free-tier accounts
# still get a low concurrent-request cap — going higher just produces 429s,
# so default stays conservative. Bump this only if you confirm your key
# allows more concurrency.
JINA_MAX_CONCURRENCY = _getenv_int("JINA_MAX_CONCURRENCY", 3)

# Per-request timeout (seconds). Jina typically responds in ~2s but complex/
# JS-heavy pages can take much longer; this is passed to Jina itself via the
# X-Timeout header AND enforced client-side so a single stuck page can never
# stall the whole run.
JINA_TIMEOUT_SECONDS = _getenv_int("JINA_TIMEOUT_SECONDS", 30)

# Retry attempts per URL on 429 / 5xx / timeout, with linear backoff.
JINA_MAX_RETRIES = _getenv_int("JINA_MAX_RETRIES", 3)
JINA_RETRY_BACKOFF_SECONDS = _getenv_float("JINA_RETRY_BACKOFF_SECONDS", 8)

# Mongo collection names
COLLECTION_JOBS = "jobs"
COLLECTION_RUNS = "pipeline_runs"

# Priority scoring weights (see README section in app.py docstring)
SCORE_WEIGHTS = {
    "direct_assessment": 2,
    "eligible_2027": 2,
    "fresher": 2,
    "cse_it": 2,
    "no_resume_screen": 2,
    "deadline_soon_bonus": 2,  # applied if deadline within 5 days
}