import requests
import logging
from typing import List, Dict
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class RemotiveScraper(BaseScraper):
    """Scraper for Remotive API (Remote jobs)"""
    
    def __init__(self):
        self.api_url = "https://remotive.com/api/remote-jobs"

    async def scrape(self, keyword: str, lang: str) -> List[Dict]:
        # Remotive is mostly English, but we filter by keyword
        params = {"search": keyword, "limit": 50}
        
        try:
            response = requests.get(self.api_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            jobs = []
            for item in data.get('jobs', []):
                jobs.append({
                    "title": item.get('title'),
                    "company": {"name": item.get('company_name'), "logo": item.get('company_logo')},
                    "description": item.get('description'),
                    "link": item.get('url'),
                    "location_raw": item.get('candidate_required_location'),
                    "source": "Remotive",
                    "original_language": "en", # Remotive is primarily EN
                    "published_at": item.get('publication_date')
                })
            return jobs
        except Exception as e:
            logger.error(f"Error scraping Remotive: {e}")
            return []
