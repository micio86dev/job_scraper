import requests
import logging
from typing import List, Dict
from datetime import datetime
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class ArbeitnowScraper(BaseScraper):
    """Scraper for Arbeitnow API"""

    def __init__(self):
        self.api_url = "https://www.arbeitnow.com/api/job-board-api"

    async def scrape(self, keyword: str, lang: str) -> List[Dict]:
        try:
            response = requests.get(self.api_url, timeout=10)
            response.raise_for_status()
            data = response.json()

            jobs = []
            for item in data.get("data", []):
                title = item.get("title", "")
                description = item.get("description", "")
                tags = item.get("tags", [])

                # Filter by keyword
                search_text = (title + " " + description + " ".join(tags)).lower()
                if keyword.lower() not in search_text:
                    continue

                # Arbeitnow dates are timestamps, e.g. 1698322633
                created_at = item.get("created_at")
                pub_date = None
                if created_at:
                    try:
                        pub_date = datetime.fromtimestamp(created_at).strftime(
                            "%Y-%m-%d"
                        )
                    except:
                        pass

                jobs.append(
                    {
                        "title": title,
                        "company": {
                            "name": item.get("company_name"),
                            "logo": None,
                        },  # Logo URL is not always direct
                        "description": description,
                        "link": item.get("url"),
                        "location_raw": item.get("location"),
                        "source": "Arbeitnow",
                        "original_language": "en",  # Mostly English/German
                        "published_at": pub_date,
                        "remote": item.get("remote", False),
                    }
                )

            return jobs

        except Exception as e:
            logger.error(f"Error scraping Arbeitnow: {e}")
            return []
