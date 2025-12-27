import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from typing import List, Dict
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class JobisJobScraper(BaseScraper):
    """Scraper for JobisJob (backup scraping)"""
    
    BASE_URLS = {
        "it": "https://www.jobisjob.it",
        "es": "https://www.jobisjob.es",
        "fr": "https://www.jobisjob.fr",
        "de": "https://www.jobisjob.de",
        "en": "https://www.jobisjob.co.uk"
    }

    async def scrape(self, keyword: str, lang: str) -> List[Dict]:
        base_url = self.BASE_URLS.get(lang, self.BASE_URLS["en"])
        url = f"{base_url}/cerca?q={keyword}"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            job_cards = soup.select('.job-offer') # This might need adjustment based on site structure
            jobs = []
            
            for card in job_cards:
                title_elem = card.select_one('.job-title')
                company_elem = card.select_one('.company')
                link_elem = card.select_one('a[href*="/job/"]')
                date_elem = card.select_one('span.date')
                
                if title_elem and link_elem:
                    date_text = date_elem.text.strip().lower() if date_elem else ""
                    # Basic normalization for "today" in different languages
                    is_today = any(word in date_text for word in ['oggi', 'today', 'hoy', 'aujourd', 'heute', 'just now', 'ora'])
                    
                    jobs.append({
                        "title": title_elem.text.strip(),
                        "company": {"name": company_elem.text.strip() if company_elem else "Unknown"},
                        "link": base_url + link_elem['href'] if link_elem['href'].startswith('/') else link_elem['href'],
                        "description": "Scraped from JobisJob. Full details at link.",
                        "source": "JobisJob (Scraping)",
                        "original_language": lang,
                        "published_at": datetime.now().strftime('%Y-%m-%d') if is_today else "older"
                    })
            return jobs
        except Exception as e:
            logger.error(f"Error scraping JobisJob ({lang}): {e}")
            return []
