import requests
import logging
import os
from datetime import datetime, timedelta
from typing import List, Dict
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class JoobleScraper(BaseScraper):
    """Scraper for Jooble API (Powerful aggregator)"""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv('JOOBLE_API_KEY')
        self.base_url = "https://jooble.org/api/"

    async def scrape(self, keyword: str, lang: str) -> List[Dict]:
        if not self.api_key:
            # logger.warning("Jooble API Key missing, skipping.")
            return []
            
        base_url = "https://it.jooble.org/api" if lang == "it" else "https://jooble.org/api"
        url = f"{base_url}/{self.api_key}"
        
        # Jooble API treats "language" by the regional endpoint, 
        # but the JSON body can specify location.
        location = "Italy" if lang == "it" else "" # Simplified
        
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        payload = {
            "keywords": keyword,
            "location": location,
            "dateFrom": (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        }
        
        try:
            # Disable SSL verification for Jooble API as it often has issues in some environments
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            
            response = requests.post(url, json=payload, headers=headers, timeout=10, verify=False)
            response.raise_for_status()
            data = response.json()
            
            jobs = []
            for item in data.get('jobs', []):
                jobs.append({
                    "title": item.get('title'),
                    "company": {"name": item.get('company') or "Unknown"},
                    "description": item.get('snippet'), #Snippet is often enough for AI
                    "link": item.get('link'),
                    "location_raw": item.get('location'),
                    "source": f"Jooble ({item.get('source', 'Unknown')})",
                    "original_language": lang,
                    "published_at": item.get('updated') # Jooble returns update date
                })
            return jobs
        except Exception as e:
            logger.error(f"Error scraping Jooble API: {e}")
            return []
