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
            logger.error("MongoDB URI is not set!")
            raise ValueError("MongoDB URI not found in environment variables")

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

            # Log connection attempt (obfuscated URI)
            safe_uri = uri.split("@")[-1] if "@" in uri else uri
            logger.info(f"Connecting to MongoDB at {safe_uri} (TLS={use_tls})")

            self.client = MongoClient(uri, **connection_args)

            # Trigger connection with Smart Fallback for Stage
            try:
                self.client.admin.command("ping")
            except Exception as e:
                # If localhost:27017 fails and it's a stage DB, try 27018 (Docker mapping)
                if "localhost" in uri and "27017" in uri and "stage" in database:
                    logger.warning(
                        f"Connection to localhost:27017 failed for stage DB. Retrying on port 27018... Error: {e}"
                    )
                    fallback_uri = uri.replace("27017", "27018")
                    self.client = MongoClient(fallback_uri, **connection_args)
                    self.client.admin.command("ping")
                    logger.info("Fallback connection to localhost:27018 successful.")
                else:
                    raise e

            self.db = self.client[database]
            logger.info(
                f"MongoDB connection established successfully to database: {database}"
            )
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
