import aiohttp
import asyncio
from bs4 import BeautifulSoup, Comment
import logging
import re
from typing import Tuple, Optional
from markdownify import markdownify as md

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
            "Accept-Language": "en-US,en;q=0.9,it;q=0.8",
        }

    async def fetch(self, url: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Fetches the URL and extracts the job description in Markdown format.
        Returns (description, logo_url) or (None, None) if extraction fails.
        """
        if not url:
            return None, None

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(
                    url, headers=self.headers, timeout=self.timeout, ssl=False
                ) as response:
                    if response.status != 200:
                        raise Exception(f"HTTP {response.status}")

                    html = await response.text()
                    return self._extract_content(html)
            except Exception as e:
                logger.error(f"Error fetching {url}: {str(e)}")
                return None, None

    def _extract_content(self, html: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Heuristic to find the main content of the job text.
        Returns (description_markdown, logo_url)
        """
        soup = BeautifulSoup(html, "html.parser")

        # Remove unwanted elements
        for element in soup(
            [
                "script",
                "style",
                "nav",
                "header",
                "footer",
                "iframe",
                "noscript",
                "meta",
                "link",
            ]
        ):
            element.decompose()

        # Remove comments
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()

        # Heuristics for container
        candidates = []

        # 1. Look for specific job description selectors common in job boards
        selectors = [
            '[class*="description"]',
            '[id*="description"]',
            '[class*="job-body"]',
            '[class*="job-content"]',
            "article",
            "main",
            '[role="main"]',
        ]

        target_container = None
        for selector in selectors:
            elements = soup.select(selector)
            for el in elements:
                text_len = len(el.get_text(strip=True))
                # Filter out small snippets
                if text_len > 300:
                    candidates.append((el, text_len))

        if candidates:
            candidates.sort(key=lambda x: x[1], reverse=True)
            target_container = candidates[0][0]

        # 3. Fallback: Body text
        if not target_container:
            body = soup.find("body")
            if body and len(body.get_text(strip=True)) > 300:
                target_container = body

        if not target_container:
            return None, None

        # Extract logo if it's an image at the very beginning
        logo_url = None
        # Look for the first img tag
        first_img = target_container.find("img")
        if first_img:
            # Check if there is significant text before this image
            # We look at the absolute position in the text representation
            text_upto_img = ""
            for sibling in first_img.previous_siblings:
                if hasattr(sibling, "get_text"):
                    text_upto_img += sibling.get_text()
                else:
                    text_upto_img += str(sibling)

            # Heuristic: if less than 50 chars of text before the first image,
            # we consider it a header/logo and extract it.
            if len(text_upto_img.strip()) < 50:
                logo_url = first_img.get("src")
                if logo_url:
                    logger.info(f"Extracted logo from description header: {logo_url}")
                    first_img.decompose()  # Remove from description content

        # REMOVE ALL OTHER IMAGES (per user request)
        for img in target_container.find_all("img"):
            img.decompose()

        # Convert to markdown
        description = self._clean_markdown(md(str(target_container)))
        return description, logo_url

    def _clean_markdown(self, text: str) -> str:
        """
        Clean whitespace and normalize markdown text.
        """
        # Collapse multiple newlines
        text = re.sub(r"\n\s*\n", "\n\n", text)
        return text.strip()
