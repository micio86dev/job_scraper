#!/usr/bin/env python3
import os
import sys
import logging
import asyncio
from datetime import datetime
from dotenv import load_dotenv

from database.mongo_client import MongoDBClient
from ai.categorizer import JobCategorizer
from utils.deduplicator import JobDeduplicator
from utils.geocoding import Geocoder
from scrapers.remotive_scraper import RemotiveScraper
from scrapers.adzuna_scraper import AdzunaScraper
from scrapers.rss_scraper import RSSScraper
from scrapers.jobisjob_scraper import JobisJobScraper

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
    def __init__(self):
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
        
        # Initialize scrapers
        self.scrapers = [
            RemotiveScraper(),
            AdzunaScraper(
                app_id=os.getenv('ADZUNA_APP_ID'),
                app_key=os.getenv('ADZUNA_APP_KEY')
            ),
            RSSScraper(rss_urls={
                "it": ["https://it.jooble.org/rss/jobs-developer"],
                "es": ["https://es.jooble.org/rss/jobs-desarrollador"],
                "fr": ["https://fr.jooble.org/rss/jobs-developpeur"],
                "de": ["https://de.jooble.org/rss/jobs-entwickler"],
                "en": ["https://jooble.org/rss/jobs-developer"]
            }),
            JobisJobScraper()
        ]
        
        self.languages = ['en', 'it', 'es', 'fr', 'de']
        self.keywords = ['python', 'javascript', 'java', 'frontend', 'backend', 'fullstack', 'devops']

    async def run(self):
        logger.info("Starting job scraper run...")
        
        for scraper in self.scrapers:
            for lang in self.languages:
                for keyword in self.keywords:
                    logger.info(f"Scraping {scraper.__class__.__name__} for {keyword} in {lang}")
                    try:
                        jobs = await scraper.scrape(keyword, lang)
                        logger.info(f"Found {len(jobs)} potential jobs")
                        
                        for job in jobs:
                            # 1. Deduplicate
                            if self.deduplicator.is_duplicate(job):
                                continue
                            
                            # 2. AI Categorize
                            logger.info(f"Processing job: {job['title']}")
                            ai_data = await self.categorizer.categorize_job(job['title'], job['description'])
                            
                            if ai_data:
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
                                self.db_client.insert_job(job)
                                logger.info(f"Saved new job: {job['title']}")
                            
                            # Rate limiting for AI API
                            await asyncio.sleep(1)
                            
                    except Exception as e:
                        logger.error(f"Error in scraper loop: {e}")
        
        self.db_client.close()
        logger.info("Job scraper run finished.")

if __name__ == "__main__":
    orchestrator = JobScraperOrchestrator()
    asyncio.run(orchestrator.run())