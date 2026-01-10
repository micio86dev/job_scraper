import aiohttp
import asyncio
from bs4 import BeautifulSoup, Comment
import logging
import re

logger = logging.getLogger(__name__)

class DescriptionFetcher:
    """
    Utility to fetch and extract the main job description content from a URL.
    """
    
    def __init__(self, timeout=10):
        self.timeout = timeout
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,it;q=0.8"
        }

    async def fetch(self, url: str) -> str:
        """
        Fetches the URL and extracts the job description.
        Returns None if extraction fails or content is not found.
        """
        if not url:
            return None
            
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, timeout=self.timeout, ssl=False) as response:
                    if response.status != 200:
                        logger.warning(f"Failed to fetch description from {url}: Status {response.status}")
                        return None
                    
                    html = await response.text()
                    return self._extract_content(html)
        except Exception as e:
            logger.warning(f"Error fetching description from {url}: {str(e)}")
            return None

    def _extract_content(self, html: str) -> str:
        """
        Heuristic to find the main content of the job text.
        """
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove unwanted elements
        for element in soup(['script', 'style', 'nav', 'header', 'footer', 'iframe', 'noscript', 'meta', 'link']):
            element.decompose()
            
        # Remove comments
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()

        # Heuristics for container
        candidates = []
        
        # 1. Look for specific job description selectors common in job boards
        selectors = [
            '[class*="description"]', '[id*="description"]',
            '[class*="job-body"]', '[class*="job-content"]',
            'article', 'main', '[role="main"]'
        ]
        
        for selector in selectors:
            elements = soup.select(selector)
            for el in elements:
                text_len = len(el.get_text(strip=True))
                # Filter out small snippets (navbars, footers usually small or too dispersed)
                if text_len > 300: 
                    candidates.append((el, text_len))

        # 2. Sort by length, assuming description is the largest text block
        if candidates:
            candidates.sort(key=lambda x: x[1], reverse=True)
            best_candidate = candidates[0][0]
            return self._clean_text(best_candidate.get_text(separator='\n\n'))

        # 3. Fallback: Body text if nothing else matches (risky, might get garbage)
        body = soup.find('body')
        if body and len(body.get_text(strip=True)) > 300:
             return self._clean_text(body.get_text(separator='\n\n'))

        return None

    def _clean_text(self, text: str) -> str:
        """
        Clean whitespace and normalize text.
        """
        # Collapse multiple newlines
        text = re.sub(r'\n\s*\n', '\n\n', text)
        # Collapse multiple spaces
        text = re.sub(r'[ \t]+', ' ', text)
        return text.strip()
