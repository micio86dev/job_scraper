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
            # Detect if we should use TLS (default for Atlas, maybe not for local)
            use_tls = "localhost" not in uri and "127.0.0.1" not in uri

            # For debugging SSL errors on macOS
            connection_args = {
                "connectTimeoutMS": 10000,
                "socketTimeoutMS": 10000,
                "serverSelectionTimeoutMS": 10000,
            }

            if use_tls:
                connection_args["tls"] = True
                connection_args["tlsCAFile"] = certifi.where()
            else:
                connection_args["tls"] = False

            self.client = MongoClient(uri, **connection_args)
            # Trigger connection
            self.client.admin.command("ping")
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
        name = company_data.get("name")
        if not name:
            return None

        # Normalize logo field - use logo_url as primary
        logo = company_data.get("logo") or company_data.get("logo_url")

        # Fields to update on every upsert (if provided)
        update_fields = {"name": name}
        if logo:
            update_fields["logo_url"] = logo
            update_fields["logo"] = logo
        if company_data.get("description"):
            update_fields["description"] = company_data.get("description")
        if company_data.get("website"):
            update_fields["website"] = company_data.get("website")
        if company_data.get("industry"):
            update_fields["industry"] = company_data.get("industry")
        if company_data.get("size"):
            update_fields["size"] = company_data.get("size")
        if company_data.get("location"):
            update_fields["location"] = company_data.get("location")

        # Fields to set only on insert (defaults)
        insert_defaults = {
            "created_at": datetime.utcnow(),
            "trustScore": 80.0,
            "totalRatings": 0,
            "totalLikes": 0,
            "totalDislikes": 0,
        }

        # Use find_one_and_update with upsert=True to get the ID
        result = self.companies.find_one_and_update(
            {"name": name},
            {"$set": update_fields, "$setOnInsert": insert_defaults},
            upsert=True,
            return_document=True,
        )
        return result["_id"]

    def upsert_seniority(self, level: str) -> ObjectId:
        """Upsert seniority level and return its ID"""
        if not level:
            level = "Unknown"

        result = self.seniorities.find_one_and_update(
            {"level": level},
            {
                "$set": {"level": level},
                "$setOnInsert": {"created_at": datetime.utcnow()},
            },
            upsert=True,
            return_document=True,
        )
        return result["_id"]

    def insert_job(self, job_data: Dict) -> Optional[ObjectId]:
        """Insert a job if it doesn't exist by link"""
        try:
            job_data["created_at"] = datetime.utcnow()
            result = self.jobs.insert_one(job_data)
            return result.inserted_id
        except Exception:
            # Likely duplicate link
            return None

    def close(self):
        self.client.close()
