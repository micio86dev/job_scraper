#!/usr/bin/env python3
import os, re, hashlib, requests, logging, time, json
from datetime import datetime, timedelta, timezone
from typing import List, Any, Optional
from langdetect import detect, DetectorFactory, LangDetectException
from pymongo import MongoClient
from dateutil import parser as dtparser
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import spacy
import certifi

DetectorFactory.seed = 0

# ============ CONFIG ============
load_dotenv()
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
JOOBLE_KEY = os.getenv("JOOBLE_KEY")
AI_BACKEND = os.getenv("AI_BACKEND", "openai")  # openai | ollama | fallback

LANG_WHITELIST = {"en", "es", "fr", "de", "it"}

try:
    NLP = spacy.load("en_core_web_sm")
except OSError:
    print("⚠️ Spacy model not found. Downloading...")
    os.system("python -m spacy download en_core_web_sm")
    NLP = spacy.load("en_core_web_sm")

TECH_SKILLS = {
    "python", "java", "javascript", "typescript", "react", "vue", "vuejs",
    "angular", "node", "bun", "django", "socket", "flask", "fastapi",
    "firebase", "css", "html", "flutter", "reactnative", "nativescript",
    "rest", "api", "c++", "c#", "c", "rust", "rocket", "qwik", "go",
    "ruby", "rails", "php", "laravel", "spring", "sql", "mongodb",
    "terraform", "git", "mysql", "postgres", "gcp", "redis", "docker",
    "kubernetes", "aws", "azure",
}

SENIORITY_KEYWORDS = {
    "intern": "intern",
    "junior": "junior",
    "mid": "mid",
    "senior": "senior",
    "lead": "lead",
    "principal": "principal",
}

# ============ LOGGING ============
log_file = os.path.join(os.path.dirname(__file__), "jobs_import.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ============ AI EXTRACTION (NEW!) ============

def extract_with_openai(title: str, description: str) -> dict:
    """Extract structured data using OpenAI API"""
    try:
        import openai
    except ImportError:
        logging.error("OpenAI not installed. Run: pip install openai")
        return {}
    
    openai.api_key = os.getenv("OPENAI_API_KEY")
    
    if not openai.api_key:
        logging.error("OPENAI_API_KEY not found in .env")
        return {}
    
    # Prepare tech skills list for prompt
    skills_list = ", ".join(sorted(TECH_SKILLS))
    
    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{
                "role": "system",
                "content": "You are an expert HR analyst. Extract structured data from job postings. Return ONLY valid JSON."
            }, {
                "role": "user",
                "content": f"""Analyze this job posting:

Title: {title}
Description: {description[:3000]}

Extract as JSON:
{{
  "skills": ["python", "react", "docker"],
  "seniority": "junior|mid|senior|lead|intern|null",
  "remote": true,
  "salary_min": 30000,
  "salary_max": 50000,
  "location": "City, Country",
  "requirements": ["requirement1", "requirement2"],
  "benefits": ["benefit1", "benefit2"],
  "language": "it|en|es|fr|de"
}}

Rules:
- skills: ONLY extract skills from this list (lowercase): {skills_list}
- Look for these exact terms in the job description
- seniority: choose best match or null
- salary: numbers only, null if not found
- remote: true if mentions remote/work from home/remoto/télétravail
- language: detect main language of description (it/en/es/fr/de)"""
            }],
            response_format={"type": "json_object"},
            temperature=0.1
        )
        
        result = json.loads(response.choices[0].message.content)
        
        # Filter skills to ensure they're in TECH_SKILLS
        if result.get("skills"):
            result["skills"] = [s.lower() for s in result["skills"] if s.lower() in TECH_SKILLS]
        
        result['ai_backend'] = 'openai'
        return result
        
    except Exception as e:
        logging.error(f"OpenAI extraction failed: {e}")
        return {}


def extract_with_ollama(title: str, description: str) -> dict:
    """Extract structured data using local Ollama"""
    try:
        import ollama
    except ImportError:
        logging.error("Ollama not installed. Run: pip install ollama")
        return {}
    
    prompt = f"""Analyze this job posting and extract information as valid JSON only.

Title: {title}
Description: {description[:3000]}

Extract and return ONLY valid JSON:
{{
  "skills": ["python", "react"],
  "seniority": "junior|mid|senior|lead|intern|null",
  "remote": true,
  "salary_min": 30000,
  "salary_max": 50000,
  "location": "Milano, Italy",
  "requirements": ["Bachelor's degree"],
  "benefits": ["Remote work"],
  "language": "it"
}}"""

    try:
        response = ollama.chat(
            model='llama3.2',
            messages=[{'role': 'user', 'content': prompt}],
            format='json',
            options={'temperature': 0.1}
        )
        
        result = json.loads(response['message']['content'])
        result['ai_backend'] = 'ollama'
        return result
        
    except Exception as e:
        logging.error(f"Ollama extraction failed: {e}")
        return {}


def extract_job_data_with_ai(title: str, description: str) -> dict:
    """Main AI extraction - returns dict with extracted fields"""
    
    if not title or not description:
        return {}
    
    backend = AI_BACKEND.lower()
    
    if backend == "openai":
        return extract_with_openai(title, description)
    elif backend == "ollama":
        return extract_with_ollama(title, description)
    else:
        # No AI, will use fallback extraction
        return {}


# ============ HELPERS ============
def hash_text(s: str) -> str:
    return hashlib.sha1((s or "").encode()).hexdigest()


def retry_request(url, params=None, headers=None, retries=3, timeout=30):
    for attempt in range(1, retries + 1):
        try:
            return requests.get(url, params=params, headers=headers, timeout=timeout)
        except requests.exceptions.RequestException as e:
            logging.warning(f"Request failed ({attempt}/{retries}): {e}")
            time.sleep(2)
    logging.error(f"Request failed after {retries} retries: {url}")
    return None


def parse_date(raw: str) -> Optional[datetime]:
    try:
        dt = dtparser.parse(raw)
        if not dt:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def extract_tech_skills(text: str) -> List[str]:
    doc = NLP(text or "")
    found = set()
    for token in doc:
        tok_lower = token.text.lower()
        if tok_lower in TECH_SKILLS:
            found.add(tok_lower)
    return sorted(found)


def extract_email(text: str) -> Optional[str]:
    if not text:
        return None
    match = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
    if match:
        return match.group(0).lower()
    return None


def extract_seniority(text: str) -> Optional[str]:
    text_lower = (text or "").lower()
    for key, value in SENIORITY_KEYWORDS.items():
        if key in text_lower:
            return value
    return None


def extract_skills_spacy(text: str) -> List[str]:
    doc = NLP(text or "")
    skills = []
    for token in doc:
        if token.pos_ in {"PROPN", "NOUN"} and len(token.text) > 2:
            skills.append(token.text.lower())
    return sorted(set(skills))


def upsert_company(companies_col, name: str, raw: dict) -> Optional[str]:
    if not name:
        return None
    norm = re.sub(r"\W+", "", name.lower())
    
    try:
        # Try to find existing company
        company = companies_col.find_one({"normalized": norm})
        
        if company:
            # Update
            companies_col.update_one(
                {"_id": company["_id"]},
                {
                    "$set": {
                        "name": name,
                        "raw": raw,
                        "updated_at": datetime.now(),
                    }
                }
            )
            return str(company["_id"])
        else:
            # Create
            result = companies_col.insert_one({
                "name": name,
                "normalized": norm,
                "raw": raw,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            })
            return str(result.inserted_id)
    except Exception as e:
        logging.error(f"Error upserting company {name}: {e}")
        raise


def save_job(jobs_col, job: dict):
    try:
        data = {
            "title": job["title"],
            "description": job["description"],
            "company_id": job["company_ref"],
            "published": job["published"],
            "salary_min": job.get("salary_min"),
            "salary_max": job.get("salary_max"),
            "location": job.get("location"),
            "remote": job.get("remote", False),
            "skills": job["skills"],
            "experience_level": job["seniority"],
            "language": job["language"],
            "email": job.get("email"),
            "link": job["link"],
            "raw": job["raw"],
            "updated_at": datetime.now(),
            "requirements": job.get("requirements", []),
            "benefits": job.get("benefits", []),
            "ai_backend": job.get("ai_backend"),
        }

        # Use upsert to either insert or update
        result = jobs_col.update_one(
            {"link": job["link"]},
            {
                "$set": data,
                "$setOnInsert": {"inserted_at": datetime.now()}
            },
            upsert=True
        )
    except Exception as e:
        logging.error(f"Error saving job {job.get('link')}: {e}")
        raise


# ============ FETCHERS ============
def fetch_adzuna(country="gb", what="developer") -> List[dict]:
    if not ADZUNA_APP_ID:
        return []
    url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/1"
    params = {
        "app_id": ADZUNA_APP_ID,
        "app_key": ADZUNA_APP_KEY,
        "results_per_page": 50,
        "what": what,
    }
    try:
        r = requests.get(url, params=params, timeout=30).json()
    except Exception as e:
        logging.error(f"Adzuna fetch error: {e}")
        return []
        
    out = []
    for j in r.get("results", []):
        out.append({
            "link": j.get("redirect_url"),
            "title": j.get("title"),
            "description": j.get("description"),
            "company": j.get("company", {}).get("display_name"),
            "published": j.get("created"),
            "salary": j.get("salary_max"),
            "raw": j,
        })
    return out


def fetch_jooble() -> List[dict]:
    if not JOOBLE_KEY:
        print("⚠️ Nessuna JOOBLE_KEY trovata, salto Jooble")
        return []

    try:
        import http.client
        conn = http.client.HTTPConnection("jooble.org")
        headers = {"Content-type": "application/json"}
        body = json.dumps({"keywords": "developer", "location": ""})
        conn.request("POST", f"/api/{JOOBLE_KEY}", body, headers)
        res = conn.getresponse()
        data_raw = res.read()
        if res.status != 200:
            print(f"❌ Jooble error {res.status}: {data_raw[:200]}")
            return []
        data = json.loads(data_raw)
        return [
            {
                "link": j.get("link"),
                "title": j.get("title"),
                "description": j.get("snippet"),
                "company": j.get("company"),
                "published": j.get("updated"),
                "salary": j.get("salary"),
                "raw": j,
            }
            for j in data.get("jobs", [])
        ]
    except Exception as e:
        print("❌ Errore fetch_jooble:", e)
        return []


def fetch_remotive() -> List[dict]:
    try:
        r = requests.get(
            "https://remotive.com/api/remote-jobs?category=software-dev", timeout=30
        ).json()
    except Exception as e:
        logging.error(f"Remotive fetch error: {e}")
        return []
        
    return [
        {
            "link": j.get("url"),
            "title": j.get("title"),
            "description": j.get("description"),
            "company": j.get("company_name"),
            "published": j.get("publication_date"),
            "salary": j.get("salary"),
            "raw": j,
        }
        for j in r.get("jobs", [])
    ]


def fetch_rss(url: str) -> List[dict]:
    r = retry_request(url)
    if not r:
        return []
    try:
        soup = BeautifulSoup(r.text, "xml")
    except Exception as e:
        logging.error(f"Error parsing RSS {url}: {e}")
        return []

    out = []
    for it in soup.find_all("item"):
        out.append({
            "link": it.link.text if it.link else None,
            "title": it.title.text if it.title else None,
            "description": it.description.text if it.description else "",
            "company": None,
            "published": it.pubDate.text if it.pubDate else None,
            "salary": None,
            "raw": {},
        })
    return out


# Removed non-working scrapers - keeping only reliable API/RSS sources


# ============ PIPELINE (MODIFIED WITH AI!) ============
def process_job(companies_col, jobs_col, raw: dict):
    pub = parse_date(raw.get("published") or "")
    now = datetime.now(timezone.utc)
    if not pub or pub < now - timedelta(days=60):  # Changed to 60 days
        return

    desc = raw.get("description") or ""
    title = raw.get("title") or ""

    if not title or not desc:
        return

    # ===== AI EXTRACTION (NEW!) =====
    ai_data = extract_job_data_with_ai(title, desc)
    
    # If AI extraction worked, use it
    if ai_data and ai_data.get("skills"):
        skills = ai_data.get("skills", [])
        seniority = ai_data.get("seniority")
        remote = ai_data.get("remote", False)
        salary_min = ai_data.get("salary_min")
        salary_max = ai_data.get("salary_max")
        location = ai_data.get("location")
        requirements = ai_data.get("requirements", [])
        benefits = ai_data.get("benefits", [])
        lang = ai_data.get("language")
        ai_backend = ai_data.get("ai_backend", "none")
    else:
        # Fallback to original extraction methods
        skills = extract_tech_skills(desc)
        if not skills:
            skills = extract_skills_spacy(desc)
        
        seniority = extract_seniority(desc + " " + title)
        if not seniority:
            title_lower = title.lower()
            if "junior" in title_lower:
                seniority = "junior"
            elif "senior" in title_lower:
                seniority = "senior"
            elif re.search(r"\b(i|ii|iii|iv|v)\b", title_lower):
                seniority = "mid"
        
        remote = False
        salary_min = None
        salary_max = None
        location = None
        requirements = []
        benefits = []
        ai_backend = "fallback"
        
        # Language detection
        try:
            lang = detect(desc)
        except LangDetectException:
            lang = None
        if lang not in LANG_WHITELIST:
            lang = None

    # Company
    comp_ref = upsert_company(companies_col, raw.get("company"), raw.get("raw") or {})

    job_doc = {
        "link": raw.get("link"),
        "title": title,
        "description": desc,
        "company": raw.get("company"),
        "company_ref": comp_ref,
        "published": pub,
        "salary": raw.get("salary"),
        "salary_min": salary_min,
        "salary_max": salary_max,
        "location": location,
        "remote": remote,
        "skills": sorted(set(skills)),
        "seniority": seniority,
        "language": lang,
        "email": extract_email(desc),
        "requirements": requirements,
        "benefits": benefits,
        "ai_backend": ai_backend,
        "raw": raw,
    }

    print(
        f"[IMPORT] {title[:40]}... @ {raw.get('company', 'N/A')[:20]} | {seniority or 'N/A'} | {lang or 'N/A'} | AI:{ai_backend}"
    )
    save_job(jobs_col, job_doc)


def run():
    # Initialize MongoDB client
    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB")
    
    if not mongo_uri:
        print("❌ MONGO_URI not found in .env file")
        return
    
    if not db_name:
        print("⚠️  MONGO_DB not found in .env file, will try to use default database from URI")
    
    print("=" * 70)
    print("🚀 JOB IMPORTER WITH AI EXTRACTION")
    print("=" * 70)
    print(f"🤖 AI Backend: {AI_BACKEND}")
    print(f"🔗 Connecting to MongoDB...")
    print(f"   Database: {db_name or 'default from URI'}")
    
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        client.admin.command('ping')
        print("✅ Connected to database")
        
        # Get database and collections
        if db_name:
            db = client[db_name]
        else:
            db = client.get_default_database()
            
        companies_col = db["Company"]
        jobs_col = db["Job"]
        
        # Test connection
        test_count = companies_col.count_documents({})
        print(f"📊 Current companies in DB: {test_count}")
        
        all_jobs = []
        
        # Fetch from all sources
        print("\n🔍 Fetching jobs from sources...")
        
        # International sources
        for country in ["gb", "fr", "de", "it", "es"]:
            jobs = fetch_adzuna(country)
            print(f"  - Adzuna {country}: {len(jobs)} jobs")
            all_jobs += jobs
            
        jooble_jobs = fetch_jooble()
        print(f"  - Jooble: {len(jooble_jobs)} jobs")
        all_jobs += jooble_jobs
        
        remotive_jobs = fetch_remotive()
        print(f"  - Remotive: {len(remotive_jobs)} jobs")
        all_jobs += remotive_jobs
        
        # RSS feeds
        print("\n📡 Fetching from RSS feeds...")
        rss_jobs = fetch_rss("https://weworkremotely.com/categories/remote-programming-jobs.rss")
        print(f"  - WeWorkRemotely: {len(rss_jobs)} jobs")
        all_jobs += rss_jobs
        
        rss_jobs = fetch_rss("https://remoteok.com/remote-dev-jobs.rss")
        print(f"  - RemoteOK: {len(rss_jobs)} jobs")
        all_jobs += rss_jobs
        
        rss_jobs = fetch_rss("https://www.python.org/jobs/feed/rss")
        print(f"  - Python.org: {len(rss_jobs)} jobs")
        all_jobs += rss_jobs

        print(f"\n📦 Total jobs fetched: {len(all_jobs)}")
        print(f"💾 Processing with AI ({AI_BACKEND})...\n")

        # Process all jobs
        success_count = 0
        error_count = 0
        
        for idx, j in enumerate(all_jobs, 1):
            try:
                process_job(companies_col, jobs_col, j)
                success_count += 1
                if idx % 10 == 0:
                    print(f"  📊 Progress: {idx}/{len(all_jobs)} jobs processed")
            except Exception as e:
                error_count += 1
                print(f"❌ Error on job {j.get('title', 'Unknown')[:50]}: {str(e)[:100]}")
                logging.error(f"Error processing job {j.get('link')}: {e}")

        print(f"\n✅ Import completed!")
        print(f"   ✓ Success: {success_count}")
        print(f"   ✗ Errors: {error_count}")
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        logging.error(f"Fatal error in run(): {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'client' in locals():
            print("\n🔌 Disconnecting from database...")
            client.close()
            print("✅ Disconnected")


if __name__ == "__main__":
    run()