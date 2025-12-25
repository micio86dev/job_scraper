from abc import ABC, abstractmethod
from typing import List, Dict

class BaseScraper(ABC):
    @abstractmethod
    async def scrape(self, keyword: str, lang: str) -> List[Dict]:
        pass
