#!/usr/bin/env python3
"""Debug Indeed.it HTML structure"""
import requests
from bs4 import BeautifulSoup

url = "https://it.indeed.com/jobs?q=developer&l=italia"
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

r = requests.get(url, headers=headers, timeout=30)
soup = BeautifulSoup(r.text, "html.parser")

# Save HTML to file for inspection
with open("indeed_debug.html", "w", encoding="utf-8") as f:
    f.write(soup.prettify())

print(f"✅ Saved HTML to indeed_debug.html ({len(r.text)} bytes)")

# Try to find job cards with different selectors
selectors_to_try = [
    "div.job_seen_beacon",
    "div.cardOutline",
    "div.slider_container",
    "div.job_seen_beacon",
    "div[class*='job']",
    "li.eu4oa1w0",
    "div.resultContent",
]

print("\n🔍 Testing different selectors:")
for selector in selectors_to_try:
    jobs = soup.select(selector)
    print(f"  {selector}: {len(jobs)} elements found")
    if jobs and len(jobs) > 0:
        print(f"    First element classes: {jobs[0].get('class', [])}")
