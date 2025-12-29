import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from typing import List, Dict
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class InfoJobsScraper(BaseScraper):
    """Scraper for InfoJobs.it"""
    
    def __init__(self):
        self.base_url = "https://www.infojobs.it"

    async def scrape(self, keyword: str, lang: str) -> List[Dict]:
        if lang != "it":
            return [] # InfoJobs is specific to Italy (and Spain, but here we target IT)
            
        url = f"{self.base_url}/offerte-lavoro?keyword={keyword}"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            job_cards = soup.select('.ij-OfferCardContent')
            if not job_cards:
                job_cards = soup.select('div[class*="ij-OfferCardContent"]')

            jobs = []
            for card in job_cards:
                title_elem = card.select_one('a[class*="title-link"]') or card.select_one('.ij-OfferCardContent-description-title a')
                company_elem = card.select_one('a[class*="subtitle-link"]') or card.select_one('.ij-OfferCardContent-description-subtitle a')
                
                # Check for date in multiple possible locations
                date_text = ""
                date_elem = card.select_one('.ij-OfferCardContent-description-date')
                if date_elem:
                    date_text = date_elem.text.strip().lower()
                else:
                    ul_elem = card.select_one('ul[class*="description-list"]')
                    if ul_elem:
                        list_items = ul_elem.find_all('li')
                        if list_items:
                            date_text = list_items[-1].text.strip().lower()

                if title_elem and title_elem.get('href'):
                    # Normalize "today" for InfoJobs (e.g. "oggi", "1h fa", etc.)
                    is_today = any(word in date_text for word in ['oggi', 'ora', 'h fa', 'm fa', 'just now', 'minuti', 'secondi'])
                    
                    jobs.append({
                        "title": title_elem.text.strip(),
                        "company": {"name": company_elem.text.strip() if company_elem else "Unknown"},
                        "link": title_elem['href'] if title_elem['href'].startswith('http') else self.base_url + title_elem['href'],
                        "description": "Scraped from InfoJobs.it. Full details at link.",
                        "source": "InfoJobs",
                        "original_language": "it",
                        "published_at": datetime.now().strftime('%Y-%m-%d') if is_today else "older"
                    })
            return jobs
        except Exception as e:
            logger.error(f"Error scraping InfoJobs: {e}")
            return []
