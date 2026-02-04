import requests
import logging
import feedparser
from typing import List, Dict
from .base_scraper import BaseScraper
from datetime import datetime

logger = logging.getLogger(__name__)


class JobsColliderScraper(BaseScraper):
    """
    Scraper for JobsCollider.
    Primary method: RSS Feed (public and realtime-ish).
    Fallback/Alternative: API (if implemented in future).
    """

    def __init__(self, use_api=False, api_token=None):
        self.rss_url = "https://jobscollider.com/remote-jobs.rss"
        self.use_api = use_api
        self.api_token = api_token

    async def scrape(self, keyword: str, lang: str) -> List[Dict]:
        """
        Scrape JobsCollider.
        Note: JobsCollider is primarily Remote, so 'lang' might be less relevant
        unless filtering by description content.
        """
        # For now, we only implement RSS scraping as it is the most reliable free method
        return self._scrape_rss(keyword, lang)

    def _scrape_rss(self, keyword: str, lang: str) -> List[Dict]:
        logger.info(f"Scraping JobsCollider RSS for keyword: {keyword}")
        jobs = []

        # Try category specific feed first if keyword maps to one
        urls_to_try = [self.rss_url]
        if "software" in keyword.lower() or "developer" in keyword.lower():
            # Based on docs, but currently returning 404. Keeping for future proofing.
            urls_to_try.insert(
                0, "https://jobscollider.com/remote-jobs/software-development.rss"
            )

        for url in urls_to_try:
            try:
                feed = feedparser.parse(url)
                if feed.status == 404:
                    logger.warning(f"JobsCollider RSS 404 for {url}")
                    continue

                if not feed.entries:
                    logger.info(f"JobsCollider RSS empty for {url}")
                    continue

                for entry in feed.entries:
                    title = entry.title

                    # Basic Keyword Matching
                    if (
                        keyword.lower() not in title.lower()
                        and keyword.lower() not in entry.description.lower()
                    ):
                        continue

                    # Published Date Parsing
                    pub_date = None
                    if hasattr(entry, "published_parsed"):
                        pub_date = datetime(*entry.published_parsed[:6])

                    job = {
                        "title": title,
                        "company": {
                            "name": entry.get("author", "JobsCollider"),
                            "logo": None,
                        },
                        "description": self.clean_description(entry.description),
                        "link": entry.link,
                        "source": "JobsCollider",
                        "original_language": lang,
                        "published_at": pub_date,
                        "remote": True,
                        "location": "Remote",
                        "salary_min": None,
                        "salary_max": None,
                        "currency": None,
                    }
                    jobs.append(job)

                # If we found jobs in the specific category, stop there
                if jobs:
                    break

            except Exception as e:
                logger.error(f"Error scraping JobsCollider RSS {url}: {e}")

        return jobs
