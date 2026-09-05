"""
crawl_raw_dump.py
------------------
Standalone utility: crawl config.SEED_URLS (and optionally
config.SEARCH_QUERIES turned into search-result URLs) via the same Jina AI
Reader crawler app.py uses, and dump the RAW output to a .json file. No
Gemini, no Mongo, no email — this is purely for inspecting what content is
actually coming back before it goes anywhere near the extraction/eligibility
pipeline.

Usage:
    python file.py                     # seed URLs only
    python file.py --include-search    # seed URLs + search queries
    python file.py --out my_dump.json
    python file.py --limit 10          # only crawl first N URLs (quick test)

Output JSON shape:
{
  "generated_at": "2026-09-04T12:34:56.789Z",
  "total_urls_attempted": 57,
  "total_succeeded": 54,
  "total_failed": 3,
  "results": [
    {"url": "https://...", "success": true, "markdown": "..."},
    {"url": "https://...", "success": false, "markdown": null}
  ]
}
"""

import argparse
import asyncio
import json
import logging
from datetime import datetime, timezone

import config
# Reuse the exact same Jina-based crawler app.py uses in production, so this
# debug dump reflects real behavior (rate limiting, concurrency cap, retries).
from app import build_search_urls, crawl_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("crawl_raw_dump")


def main():
    parser = argparse.ArgumentParser(description="Crawl seed URLs (and optionally search queries) via Jina Reader and dump raw JSON.")
    parser.add_argument("--include-search", action="store_true", help="Also crawl config.SEARCH_QUERIES as search-result URLs.")
    parser.add_argument("--out", default="raw_crawl_dump.json", help="Output JSON file path.")
    parser.add_argument("--limit", type=int, default=None, help="Only crawl the first N URLs (useful for a quick test).")
    args = parser.parse_args()

    urls = list(config.SEED_URLS)
    if args.include_search:
        urls += build_search_urls()

    if args.limit:
        urls = urls[: args.limit]

    log.info(
        "Crawling %d URLs via Jina Reader (include_search=%s, rate_limit=%d/min, concurrency=%d)...",
        len(urls), args.include_search, config.JINA_RATE_LIMIT_PER_MINUTE, config.JINA_MAX_CONCURRENCY,
    )
    crawled: dict[str, str] = asyncio.run(crawl_all(urls))

    results = [
        {"url": u, "success": u in crawled, "markdown": crawled.get(u)}
        for u in urls
    ]
    succeeded = len(crawled)
    failed = len(urls) - succeeded

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_urls_attempted": len(urls),
        "total_succeeded": succeeded,
        "total_failed": failed,
        "results": results,
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    log.info("Done: %d succeeded, %d failed. Written to %s", succeeded, failed, args.out)


if __name__ == "__main__":
    main()
