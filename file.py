"""
crawl_raw_dump.py
------------------
Standalone utility: crawl config.SEED_URLS (and optionally
config.SEARCH_QUERIES turned into search-result URLs) with Crawl4AI, and
dump the RAW crawl output to a .json file. No Gemini, no Mongo, no email —
this is purely for inspecting what content Crawl4AI is actually pulling
back before it goes anywhere near the extraction/eligibility pipeline.

Usage:
    python crawl_raw_dump.py                     # seed URLs only
    python crawl_raw_dump.py --include-search     # seed URLs + search queries
    python crawl_raw_dump.py --out my_dump.json
    python crawl_raw_dump.py --limit 10           # only crawl first N URLs (quick test)

Output JSON shape:
{
  "generated_at": "2026-09-04T12:34:56.789Z",
  "total_urls_attempted": 57,
  "total_succeeded": 54,
  "total_failed": 3,
  "results": [
    {
      "url": "https://...",
      "success": true,
      "status_code": 200,
      "markdown": "...",        // primary content (Crawl4AI's cleaned markdown)
      "cleaned_html": "...",    // fallback if markdown is empty
      "error": null
    },
    {
      "url": "https://...",
      "success": false,
      "status_code": null,
      "markdown": null,
      "cleaned_html": null,
      "error": "TimeoutError: ..."
    }
  ]
}
"""

import argparse
import asyncio
import json
import logging
from datetime import datetime, timezone
from urllib.parse import quote_plus

from crawl4ai import AsyncWebCrawler

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("crawl_raw_dump")


def build_search_urls() -> list[str]:
    """Same logic as app.py's build_search_urls: turn SEARCH_QUERIES into result-page URLs."""
    return [
        f"https://www.google.com/search?q={quote_plus(q)}&num=20"
        for q in config.SEARCH_QUERIES
    ]


async def crawl_one(crawler: AsyncWebCrawler, url: str, semaphore: asyncio.Semaphore, timeout: int) -> dict:
    async with semaphore:
        try:
            result = await asyncio.wait_for(crawler.arun(url=url), timeout=timeout)
            if result and result.success:
                return {
                    "url": url,
                    "success": True,
                    "status_code": getattr(result, "status_code", None),
                    "markdown": result.markdown or "",
                    "cleaned_html": result.cleaned_html or "",
                    "error": None,
                }
            else:
                error_msg = getattr(result, "error_message", "crawl reported success=False")
                log.warning("Crawl failed for %s: %s", url, error_msg)
                return {
                    "url": url,
                    "success": False,
                    "status_code": getattr(result, "status_code", None) if result else None,
                    "markdown": None,
                    "cleaned_html": None,
                    "error": str(error_msg),
                }
        except Exception as exc:  # noqa: BLE001
            log.warning("Crawl error for %s: %s", url, exc)
            return {
                "url": url,
                "success": False,
                "status_code": None,
                "markdown": None,
                "cleaned_html": None,
                "error": str(exc),
            }


async def crawl_all(urls: list[str]) -> list[dict]:
    semaphore = asyncio.Semaphore(config.CRAWL_CONCURRENCY)
    async with AsyncWebCrawler(verbose=False) as crawler:
        tasks = [crawl_one(crawler, u, semaphore, config.CRAWL_TIMEOUT_SECONDS) for u in urls]
        return await asyncio.gather(*tasks)


def main():
    parser = argparse.ArgumentParser(description="Crawl seed URLs (and optionally search queries) and dump raw JSON.")
    parser.add_argument("--include-search", action="store_true", help="Also crawl config.SEARCH_QUERIES as search-result URLs.")
    parser.add_argument("--out", default="raw_crawl_dump.json", help="Output JSON file path.")
    parser.add_argument("--limit", type=int, default=None, help="Only crawl the first N URLs (useful for a quick test).")
    args = parser.parse_args()

    urls = list(config.SEED_URLS)
    if args.include_search:
        urls += build_search_urls()

    if args.limit:
        urls = urls[: args.limit]

    log.info("Crawling %d URLs (include_search=%s)...", len(urls), args.include_search)
    results = asyncio.run(crawl_all(urls))

    succeeded = sum(1 for r in results if r["success"])
    failed = len(results) - succeeded

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