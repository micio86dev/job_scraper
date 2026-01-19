import logging
import requests
from typing import List, Dict
from .base_scraper import BaseScraper
from datetime import datetime

logger = logging.getLogger(__name__)


class TechMapScraper(BaseScraper):
    """
    Scraper for TechMap API.
    Requires TECHMAP_API_TOKEN.
    """

    def __init__(self, api_token=None):
        self.api_token = api_token
        self.base_url = (
            "https://api.techmap.io/v1/jobs"  # Verify endpoint documentation
        )

    async def scrape(self, keyword: str, lang: str) -> List[Dict]:
        if not self.api_token:
            logger.warning("TechMap API Token not provided. Skipping TechMapScraper.")
            return []

        logger.info(f"Scraping TechMap API for {keyword} in {lang}")

        # Mapping ISO codes if necessary. TechMap might use 2 letter codes.
        location_map = {
            "it": "Italy",
            "en": "United States",  # generic default
            "us": "United States",
            "uk": "United Kingdom",
            "de": "Germany",
            "fr": "France",
            "es": "Spain",
        }

        location = location_map.get(lang, "")

        params = {
            "query": keyword,
            "location": location,
            "per_page": 20,
            # "date_posted": "today" # Example parameter, check docs
        }

        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Accept": "application/json",
        }

        jobs = []
        try:
            response = requests.get(
                self.base_url, params=params, headers=headers, timeout=15
            )

            if response.status_code == 200:
                data = response.json()
                # Assuming standard JSON response structure (data or jobs key)
                # This needs to be adjusted based on actual API response
                results = data.get("data", [])

                for item in results:
                    job = {
                        "title": item.get("title"),
                        "company": {
                            "name": item.get("company_name", "Unknown"),
                            "logo": item.get("company_logo"),
                        },
                        "description": item.get("description", ""),
                        "link": item.get("url"),
                        "source": "TechMap",
                        "original_language": lang,
                        "published_at": item.get(
                            "date_posted"
                        ),  # Need parsing in main loop usually
                        "location": item.get("location_text"),
                        "remote": item.get("is_remote", False),
                        "salary_min": item.get("min_salary"),
                        "salary_max": item.get("max_salary"),
                        "currency": item.get("currency"),
                    }
                    jobs.append(job)
            elif response.status_code == 401:
                logger.error("TechMap API Unauthorized. Check your token.")
            elif response.status_code == 429:
                logger.warning("TechMap API Rate Limit Exceeded.")
            else:
                logger.error(
                    f"TechMap API Error: {response.status_code} - {response.text}"
                )

        except Exception as e:
            logger.error(f"Error scraping TechMap API: {e}")

        return jobs
