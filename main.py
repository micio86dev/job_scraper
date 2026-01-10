#!/usr/bin/env python3
import os
import sys
import logging
import asyncio
import argparse
from datetime import datetime, date
from dotenv import load_dotenv

from database.mongo_client import MongoDBClient
from ai.categorizer import JobCategorizer
from utils.deduplicator import JobDeduplicator
from utils.geocoding import Geocoder
from utils.description_fetcher import DescriptionFetcher
from scrapers.remotive_scraper import RemotiveScraper
from scrapers.adzuna_scraper import AdzunaScraper
from scrapers.rss_scraper import RSSScraper
from scrapers.jobisjob_scraper import JobisJobScraper
from scrapers.infojobs_scraper import InfoJobsScraper
from scrapers.jooble_scraper import JoobleScraper

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('job_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class JobScraperOrchestrator:
    def __init__(self, languages=None, limit_per_language=None, days_window=1):
        self.db_client = MongoDBClient(
            uri=os.getenv('MONGO_URI'),
            database=os.getenv('MONGO_DB', 'itjobhub')
        )
        self.categorizer = JobCategorizer(
            api_key=os.getenv('OPENAI_API_KEY'),
            model=os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
        )
        self.geocoder = Geocoder(api_key=os.getenv('GOOGLE_MAPS_API_KEY'))
        self.deduplicator = JobDeduplicator(self.db_client)
        self.description_fetcher = DescriptionFetcher()
        self.days_window = days_window
        
        # Initialize scrapers
        self.scrapers = [
            RemotiveScraper(),
            AdzunaScraper(
                app_id=os.getenv('ADZUNA_APP_ID'),
                app_key=os.getenv('ADZUNA_APP_KEY')
            ),
            JoobleScraper(api_key=os.getenv('JOOBLE_API_KEY')),
            InfoJobsScraper(),
            RSSScraper(rss_urls={
                "it": ["https://it.jooble.org/rss/jobs-informatica"],
                "es": ["https://es.jooble.org/rss/jobs-informatica"],
                "fr": ["https://fr.jooble.org/rss/jobs-informatique"],
                "de": ["https://de.jooble.org/rss/jobs-it"],
                "en": ["https://jooble.org/rss/jobs-it"]
            }),
            JobisJobScraper()
        ]
        
        self.languages = languages or ['en', 'it', 'es', 'fr', 'de']
        self.limit_per_language = limit_per_language
        self.keywords = [
            'software engineer', 'software developer', 'web developer', 
            'frontend', 'backend', 'fullstack', 'devops', 'mobile developer',
            'data scientist', 'data engineer', 'cloud engineer', 'sysadmin',
            'cybersecurity', 'java', 'python', 'javascript', 'php', 'ruby',
            'golang', 'cloud architect', 'solidity', 'blockchain',
            'informatica', 'it specialist', 'software', 'programmatore', 'sviluppatore'
        ]

    def parse_date(self, pub_date):
        if not pub_date:
            return None
        
        if pub_date == "older":
            return None

        if isinstance(pub_date, datetime):
            if pub_date.tzinfo:
                return pub_date.replace(tzinfo=None)
            return pub_date
        
        if isinstance(pub_date, date):
            return datetime.combine(pub_date, datetime.min.time())

        if isinstance(pub_date, str):
            pub_date = pub_date.strip()
            fmts = [
                '%Y-%m-%dT%H:%M:%SZ', 
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S', 
                '%a, %d %b %Y %H:%M:%S %z',
                '%a, %d %b %Y %H:%M:%S %Z',
                '%Y-%m-%d',
                '%d %b %Y'
            ]
            for fmt in fmts:
                try:
                    dt = datetime.strptime(pub_date, fmt)
                    if dt.tzinfo:
                        dt = dt.replace(tzinfo=None)
                    return dt
                except ValueError:
                    continue
            
            # Fallback: check if the string contains today's date in YYYY-MM-DD format
            today_str = date.today().strftime('%Y-%m-%d')
            if today_str in pub_date:
                return datetime.now()
                
        return None

    def is_published_today(self, pub_date, days_window=1):
        dt = self.parse_date(pub_date)
        if not dt:
            # If we can't parse it but it's not None, maybe it's just "today" or similar
            if isinstance(pub_date, str) and ("today" in pub_date.lower() or "oggi" in pub_date.lower()):
                return True
            return False
            
        diff = (datetime.now() - dt).days
        return diff <= days_window

    async def process_job_list(self, jobs, lang, lang_count):
        for job in jobs:
            if self.limit_per_language and lang_count >= self.limit_per_language:
                break
            
            # Check if published recently
            pub_date_raw = job.get('published_at')
            if not self.is_published_today(pub_date_raw, days_window=self.days_window):
                logger.debug(f"Skipping job (too old): {job['title']} - {pub_date_raw}")
                continue
            
            # Ensure published_at is a datetime object for the database
            job['published_at'] = self.parse_date(pub_date_raw) or datetime.utcnow()

            # Refine description if it's too short (snippet)
            desc = job.get('description', '')
            if not desc or len(desc) < 500:
                logger.info(f"Fetching full description for: {job['title']}")
                full_desc = await self.description_fetcher.fetch(job['link'])
                if full_desc:
                     job['description'] = full_desc
                     logger.info("Successfully fetched full description")
                else:
                     logger.warning("Could not fetch full description, keeping snippet")
            
            # 1. Deduplicate
            if self.deduplicator.is_duplicate(job):
                logger.debug(f"Skipping job (duplicate): {job['title']}")
                continue
            
            # 2. AI Categorize
            logger.info(f"Processing job: {job['title']}")
            ai_data = await self.categorizer.categorize_job(job['title'], job['description'])
            
            if ai_data:
                # Sanitize AI data
                if isinstance(ai_data.get('city'), list):
                    ai_data['city'] = str(ai_data['city'][0]) if ai_data['city'] else None
                elif ai_data.get('city') and not isinstance(ai_data['city'], (str, type(None))):
                    ai_data['city'] = str(ai_data['city'])

                job.update(ai_data)
                
                # 3. Geocode if address exists
                if ai_data.get('formatted_address'):
                    geo = self.geocoder.get_coordinates(ai_data['formatted_address'])
                    if geo:
                        job['location_geo'] = {
                            "type": "Point",
                            "coordinates": [geo['lng'], geo['lat']]
                        }
                        job['formatted_address_verified'] = geo['formatted_address']
                        
                # 4. Handle Company
                if job.get('company'):
                    company_id = self.db_client.upsert_company(job['company'])
                    job['company_id'] = company_id
                    
                # 5. Handle Seniority
                if job.get('seniority'):
                    seniority_id = self.db_client.upsert_seniority(job['seniority'])
                    job['seniority_id'] = seniority_id
                    
                # 6. Save Job
                if self.db_client.insert_job(job):
                    logger.info(f"Saved new job: {job['title']}")
                    lang_count += 1
                else:
                    logger.info(f"Job already exists or could not be saved: {job['title']}")
            
            # Rate limiting for AI API
            await asyncio.sleep(1)
        return lang_count

    async def run(self):
        logger.info(f"Starting job scraper run for languages: {self.languages}")
        
        for lang in self.languages:
            self.lang_count = 0
            logger.info(f"Processing language: {lang}")
            
            for scraper in self.scrapers:
                if self.limit_per_language and self.lang_count >= self.limit_per_language:
                    logger.info(f"Reached limit for {lang}, skipping remaining scrapers.")
                    break
                
                # Special handling for Adzuna to do a broad search with pagination
                if isinstance(scraper, AdzunaScraper):
                    page = 1
                    while True:
                        if self.limit_per_language and self.lang_count >= self.limit_per_language:
                            break
                            
                        logger.info(f"Scraping {scraper.__class__.__name__} page {page} for category 'it-jobs' in {lang}")
                        try:
                            jobs = await scraper.scrape(lang=lang, category="it-jobs", page=page)
                            if not jobs:
                                logger.info("No more jobs found on this page, stopping Adzuna.")
                                break
                                
                            logger.info(f"Found {len(jobs)} potential jobs")
                            old_count = self.lang_count
                            self.lang_count = await self.process_job_list(jobs, lang, self.lang_count)
                            
                            if self.lang_count == old_count:
                                logger.info("No new jobs added from this page, might be all duplicates or too old. Trying one more page.")
                                if page > 5: # Safety break
                                    break
                                    
                            page += 1
                            await asyncio.sleep(2) # Be nice to API
                        except Exception as e:
                            logger.error(f"Error in broad scraper: {e}")
                            break
                    continue

                for keyword in self.keywords:
                    if self.limit_per_language and self.lang_count >= self.limit_per_language:
                        break
                        
                    logger.info(f"Scraping {scraper.__class__.__name__} for {keyword} in {lang}")
                    try:
                        jobs = await scraper.scrape(keyword, lang)
                        logger.info(f"Found {len(jobs)} potential jobs")
                        self.lang_count = await self.process_job_list(jobs, lang, self.lang_count)
                    except Exception as e:
                        logger.error(f"Error in keyword scraper loop: {e}")
        
        self.db_client.close()
        logger.info("Job scraper run finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Job Scraper Orchestrator')
    parser.add_argument('--languages', type=str, help='Comma-separated list of languages (e.g. it,en,es)')
    parser.add_argument('--limit', type=int, help='Limit of total ads per language')
    parser.add_argument('--days', type=int, default=1, help='Lookback window in days (default: 1)')
    
    args = parser.parse_args()
    
    languages = args.languages.split(',') if args.languages else None
    
    orchestrator = JobScraperOrchestrator(
        languages=languages,
        limit_per_language=args.limit,
        days_window=args.days
    )
    asyncio.run(orchestrator.run())