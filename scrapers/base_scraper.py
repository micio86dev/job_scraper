from abc import ABC, abstractmethod
from typing import List, Dict
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)

class BaseScraper(ABC):
    @abstractmethod
    async def scrape(self, keyword: str, lang: str) -> List[Dict]:
        pass

    def clean_description(self, text: str) -> str:
        """
        Sanitize description to remove images and potentially unsafe/unwanted tags 
        while preserving text and structure.
        """
        if not text:
            return ""
            
        try:
            # Check if text looks like HTML
            if not ("<" in text and ">" in text):
                return text

            soup = BeautifulSoup(text, "html.parser")
            
            # Remove images and media
            for tag in soup.find_all(["img", "svg", "figure", "picture", "video", "audio", "iframe", "object", "embed", "script", "style"]):
                tag.decompose()
                
            # Remove elements with v: prefixes (VML) often found in RSS
            # BeautifulSoup with html.parser might handle namespaces poorly, but we can try to catch common ones
            # or just iterate all and check name
            
            return str(soup)
        except Exception as e:
            logger.warning(f"Error cleaning description: {e}")
            return text
