import certifi
from pymongo import MongoClient
import logging
from datetime import datetime
from typing import Dict, Optional
from bson import ObjectId

logger = logging.getLogger(__name__)

class MongoDBClient:
    def __init__(self, uri: str, database: str):
        if not uri:
            logger.error("MONGO_URI is not set in environment!")
            raise ValueError("MONGO_URI not found in environment variables")
            
        try:
            # For debugging SSL errors on macOS
            self.client = MongoClient(
                uri, 
                tls=True,
                tlsCAFile=certifi.where(),
                connectTimeoutMS=10000,
                socketTimeoutMS=10000,
                serverSelectionTimeoutMS=10000
            )
            # Trigger connection
            self.client.admin.command('ping')
            self.db = self.client[database]
            logger.info("MongoDB connection established successfully.")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

        self.jobs = self.db.jobs
        self.companies = self.db.companies
        self.seniorities = self.db.seniorities
        
        # Ensure indexes
        try:
            self.jobs.create_index([("link", 1)], unique=True)
            self.companies.create_index([("name", 1)], unique=True)
            self.seniorities.create_index([("level", 1)], unique=True)
        except Exception as e:
            logger.warning(f"Could not create indexes: {e}")

    def upsert_company(self, company_data: Dict) -> ObjectId:
        """Upsert company and return its ID"""
        name = company_data.get('name')
        if not name:
            return None
        
        # Use find_one_and_update with upsert=True to get the ID
        result = self.companies.find_one_and_update(
            {"name": name},
            {"$set": company_data, "$setOnInsert": {"created_at": datetime.utcnow()}},
            upsert=True,
            return_document=True
        )
        return result['_id']

    def upsert_seniority(self, level: str) -> ObjectId:
        """Upsert seniority level and return its ID"""
        if not level:
            level = "Unknown"
            
        result = self.seniorities.find_one_and_update(
            {"level": level},
            {"$set": {"level": level}, "$setOnInsert": {"created_at": datetime.utcnow()}},
            upsert=True,
            return_document=True
        )
        return result['_id']

    def insert_job(self, job_data: Dict) -> Optional[ObjectId]:
        """Insert a job if it doesn't exist by link"""
        try:
            job_data['created_at'] = datetime.utcnow()
            result = self.jobs.insert_one(job_data)
            return result.inserted_id
        except Exception:
            # Likely duplicate link
            return None

    def close(self):
        self.client.close()
