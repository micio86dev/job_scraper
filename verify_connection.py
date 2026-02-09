import os
import sys
from pymongo import MongoClient
from dotenv import load_dotenv


def verify_connection():
    load_dotenv()

    uri = (
        os.getenv("MONGO_URI_STAGE")
        or os.getenv("MONGO_URI_PROD")
        or os.getenv("MONGODB_URI")
        or os.getenv("MONGO_URI")
    )
    db_name = os.getenv("MONGO_DB")

    if not uri:
        print(
            "❌ MongoDB URI not found in environment variables (checked MONGO_URI_STAGE, MONGO_URI_PROD, MONGODB_URI, MONGO_URI)"
        )
        sys.exit(1)

    if not db_name:
        print("❌ MONGO_DB not found in environment variables")
        sys.exit(1)

    print(f"🔌 Testing connection to MongoDB...")
    print(f"   Database: {db_name}")

    try:
        # Connect
        client = MongoClient(uri)

        # Ping to check connection
        client.admin.command("ping")
        print("✅ Connection successful (Ping)")

        # Check database access
        db = client[db_name]
        collections = db.list_collection_names()
        print(f"✅ Database '{db_name}' accessible. Collections: {len(collections)}")

        # Check specific collections existence (optional but good sanity check)
        required = ["jobs", "companies"]
        missing = [c for c in required if c not in collections]

        if missing:
            print(f"⚠️  Warning: Missing expected collections: {missing}")
        else:
            print("✅ Expected collections found")

        return True

    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    verify_connection()
