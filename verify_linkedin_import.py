import asyncio
import logging
from scrapers.linkedin_scraper import LinkedInScraper
import sys

# Setup logging
logging.basicConfig(level=logging.INFO)


async def verify_import():
    print("🚀 Starting LinkedIn Import Verification...")
    scraper = LinkedInScraper(max_results=1, fetch_details=True)

    # Search for a common job to ensure results
    keyword = "software engineer"
    location = "it"  # Italy

    print(f"🔎 Searching for '{keyword}' in '{location}'...")
    jobs = await scraper.scrape(keyword, location)

    if not jobs:
        print("❌ No jobs found. Try different keywords/location or check network.")
        return

    job = jobs[0]
    print(f"✅ Found job: {job['title']} at {job['company']['name']}")
    print(f"🔗 ID: {job['external_id']}")

    print("\n⏳ Fetching full details (Description)...")
    details = await scraper.fetch_job_details(job["external_id"])

    if not details:
        print("❌ Failed to fetch details.")
        return

    description = details["description"]

    print("\n" + "=" * 60)
    print("📝 PROCESSED MARKDOWN DESCRIPTION")
    print("=" * 60)
    print(description)
    print("=" * 60 + "\n")

    # Validation Checks
    print("📊 Verification Checks:")

    # Check 1: Is it Markdown? (Simple heuristic)
    is_markdown = "#" in description or "*" in description or "-" in description
    print(f"[{'✅' if is_markdown else '❌'}] Is Markdown format")

    unwanted_phrases = [
        "Show more",
        "Show less",
        "Referrals increase your chances",
        "See who you know",
        "Sign in to create job alert",
        "Similar jobs",
        "Similar Searches",
        "People also viewed",
        "Explore collaborative articles",
    ]

    found_unwanted = []
    for phrase in unwanted_phrases:
        if phrase.lower() in description.lower():
            found_unwanted.append(phrase)

    print(f"[{'✅' if not found_unwanted else '❌'}] No unwanted phrases found")
    if found_unwanted:
        print(f"   ⚠️ Found: {found_unwanted}")

    # Check 3: No HTML tags (basic check)
    import re

    has_html = bool(re.search(r"<div|<span|<ul|<li|<button", description))
    print(f"[{'✅' if not has_html else '❌'}] No major HTML tags remaining")

    # Check 4: H1 demoted to H2 (Check if line starts with single # )
    has_h1 = bool(re.search(r"^#\s", description, re.MULTILINE))
    print(f"[{'✅' if not has_h1 else '❌'}] No H1 headers (SEO)")


if __name__ == "__main__":
    asyncio.run(verify_import())
