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
            "salary_min": None,
            "salary_max": None,
            "location": None,
            "remote": False,
            "skills": job["skills"],
            "experience_level": job["seniority"],
            "language": job["language"],
            "email": job["email"],
            "link": job["link"],
            "raw": job["raw"],
            "updated_at": datetime.now(),
            "requirements": [],
            "benefits": [],
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


def fetch_simplyhired(query="developer"):
    url = f"https://www.simplyhired.com/search?q={query}"
    try:
        r = requests.get(url, timeout=30)
    except Exception as e:
        logging.error(f"SimplyHired fetch error: {e}")
        return []
        
    soup = BeautifulSoup(r.text, "html.parser")
    out = []

    for job in soup.select("div.SerpJob-jobCard"):
        link_tag = job.select_one("a.SerpJob-link")
        title_tag = job.select_one("a.SerpJob-link span")
        company_tag = job.select_one(".JobPosting-labelWithIcon span")
        date_tag = job.select_one(".JobPosting-labelWithIcon time")
        desc_tag = job.select_one(".JobPosting-snippet")

        link = "https://www.simplyhired.com" + link_tag["href"] if link_tag else None
        out.append({
            "link": link,
            "title": title_tag.text.strip() if title_tag else None,
            "description": desc_tag.text.strip() if desc_tag else "",
            "company": company_tag.text.strip() if company_tag else None,
            "published": (
                date_tag["datetime"]
                if date_tag and date_tag.has_attr("datetime")
                else None
            ),
            "salary": None,
            "raw": {},
        })

    return out


def fetch_indeed_it_rss() -> List[dict]:
    """Fetch jobs from Indeed.it via RSS feed (more reliable than scraping)"""
    # Indeed RSS format: https://it.indeed.com/rss?q=developer&l=italia
    url = "https://it.indeed.com/rss?q=developer&l=italia"
    jobs = fetch_rss(url)
    print(f"     Indeed.it RSS returned {len(jobs)} jobs")
    return jobs


def fetch_linkedin_jobs_it() -> List[dict]:
    """
    Fetch IT jobs from LinkedIn Jobs Italia
    Note: LinkedIn heavily uses JavaScript, so we use their RSS when available
    """
    # LinkedIn doesn't provide public RSS, so we skip for now
    # Could integrate LinkedIn Jobs API if we have credentials
    return []


def fetch_tecnolavoro_it() -> List[dict]:
    """Fetch from Tecnolavoro.it - Italian tech job board"""
    url = "https://www.tecnolavoro.it/annunci-lavoro/sviluppatore"
    try:
        r = requests.get(url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    except Exception as e:
        logging.error(f"Tecnolavoro.it fetch error: {e}")
        return []
    
    soup = BeautifulSoup(r.text, "html.parser")
    out = []
    
    # Try multiple possible selectors
    job_cards = soup.select("article.job-card") or soup.select("div.job-listing") or soup.select("div[class*='job']")
    
    for job in job_cards[:50]:  # Limit to first 50
        try:
            title_tag = job.select_one("h2 a, h3 a, a.job-title")
            company_tag = job.select_one("span.company, div.company-name, span[class*='company']")
            desc_tag = job.select_one("p.description, div.description, div[class*='desc']")
            location_tag = job.select_one("span.location, div.location, span[class*='location']")
            
            link = title_tag.get("href") if title_tag else None
            if link and not link.startswith("http"):
                link = "https://www.tecnolavoro.it" + link
            
            out.append({
                "link": link,
                "title": title_tag.text.strip() if title_tag else None,
                "description": desc_tag.text.strip() if desc_tag else "",
                "company": company_tag.text.strip() if company_tag else None,
                "published": None,
                "salary": None,
                "raw": {"location": location_tag.text.strip() if location_tag else None},
            })
        except Exception as e:
            continue
    
    return out


def fetch_lavoroitalia_it() -> List[dict]:
    """Fetch from LavoroItalia.it"""
    url = "https://www.lavoroitalia.it/offerte-lavoro/informatica.html"
    try:
        r = requests.get(url, timeout=30, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    except Exception as e:
        logging.error(f"LavoroItalia.it fetch error: {e}")
        return []
    
    soup = BeautifulSoup(r.text, "html.parser")
    out = []
    
    job_cards = soup.select("div.offer-item, article.job, div[class*='job-item']")
    
    for job in job_cards[:50]:
        try:
            title_tag = job.select_one("h2 a, h3 a, a[class*='title']")
            company_tag = job.select_one("span.company, div[class*='company']")
            desc_tag = job.select_one("p, div[class*='description']")
            
            link = title_tag.get("href") if title_tag else None
            if link and not link.startswith("http"):
                link = "https://www.lavoroitalia.it" + link
            
            out.append({
                "link": link,
                "title": title_tag.text.strip() if title_tag else None,
                "description": desc_tag.text.strip() if desc_tag else "",
                "company": company_tag.text.strip() if company_tag else None,
                "published": None,
                "salary": None,
                "raw": {},
            })
        except Exception as e:
            continue
    
    return out


def fetch_monster_it_rss() -> List[dict]:
    """Fetch jobs from Monster.it via RSS - more reliable"""
    # Monster Italy tech jobs RSS
    urls = [
        "https://www.monster.it/rss/jobs/q-developer",
        "https://www.monster.it/rss/jobs/q-programmatore",
        "https://www.monster.it/rss/jobs/q-software-engineer",
    ]
    
    all_jobs = []
    for url in urls:
        try:
            jobs = fetch_rss(url)
            all_jobs.extend(jobs)
        except Exception as e:
            logging.warning(f"Error fetching Monster RSS {url}: {e}")
            continue
    
    return all_jobs


def fetch_glassdoor_it() -> List[dict]:
    """Fetch from Glassdoor Italy RSS"""
    url = "https://www.glassdoor.it/Job/developer-jobs-SRCH_KO0,9.htm?radius=100"
    # Glassdoor is heavily JS-based, skip for now unless we find RSS
    return []


# ============ PIPELINE ============
def process_job(companies_col, jobs_col, raw: dict):
    pub = parse_date(raw.get("published") or "")
    now = datetime.now(timezone.utc)
    if not pub or pub < now - timedelta(days=1):
        return

    desc = raw.get("description") or ""
    title = raw.get("title") or ""

    # skills
    skills = extract_tech_skills(desc)
    if not skills:
        skills = extract_skills_spacy(desc)

    # seniority
    seniority = extract_seniority(desc + " " + title)
    if not seniority:
        title_lower = title.lower()
        if "junior" in title_lower:
            seniority = "junior"
        elif "senior" in title_lower:
            seniority = "senior"
        elif re.search(r"\b(i|ii|iii|iv|v)\b", title_lower):
            seniority = "mid"

    # company
    comp_ref = upsert_company(companies_col, raw.get("company"), raw.get("raw") or {})

    # language
    try:
        lang = detect(desc)
    except LangDetectException:
        lang = None
    if lang not in LANG_WHITELIST:
        lang = None

    job_doc = {
        "link": raw.get("link"),
        "title": title,
        "description": desc,
        "company": raw.get("company"),
        "company_ref": comp_ref,
        "published": pub,
        "salary": raw.get("salary"),
        "skills": sorted(set(skills)),
        "seniority": seniority,
        "language": lang,
        "email": extract_email(desc),
        "raw": raw,
    }

    print(
        f"[IMPORT] {title} @ {raw.get('company')} ({pub.date()}) [{lang}] [{seniority}] -> {raw.get('link')}"
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
        
    print(f"🔗 Connecting to MongoDB...")
    print(f"   Database: {db_name or 'default from URI'}")
    
    try:
        # Connect to MongoDB - let pymongo handle TLS automatically
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
        
        # Test connection with a simple query
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
        
        sh_jobs = fetch_simplyhired("developer")
        print(f"  - SimplyHired: {len(sh_jobs)} jobs")
        all_jobs += sh_jobs
        
        # Italian job portals
        print("\n🇮🇹 Fetching from Italian job portals...")
        
        indeed_it_jobs = fetch_indeed_it_rss()
        print(f"  - Indeed.it (RSS): {len(indeed_it_jobs)} jobs")
        all_jobs += indeed_it_jobs
        
        monster_it_jobs = fetch_monster_it_rss()
        print(f"  - Monster.it (RSS): {len(monster_it_jobs)} jobs")
        all_jobs += monster_it_jobs
        
        tecnolavoro_jobs = fetch_tecnolavoro_it()
        print(f"  - Tecnolavoro.it: {len(tecnolavoro_jobs)} jobs")
        all_jobs += tecnolavoro_jobs
        
        lavoroitalia_jobs = fetch_lavoroitalia_it()
        print(f"  - LavoroItalia.it: {len(lavoroitalia_jobs)} jobs")
        all_jobs += lavoroitalia_jobs
        
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
        print("💾 Processing and saving jobs...\n")

        # Process all jobs
        success_count = 0
        error_count = 0
        
        for idx, j in enumerate(all_jobs, 1):
            try:
                process_job(companies_col, jobs_col, j)
                success_count += 1
                if idx % 10 == 0:
                    print(f"  Progress: {idx}/{len(all_jobs)} jobs processed")
            except Exception as e:
                error_count += 1
                print(f"❌ Error on job {j.get('title', 'Unknown')[:50]}: {str(e)[:100]}")
                logging.error(f"Error processing job {j.get('link')}: {e}")

        print(f"\n✅ Import completed!")
        print(f"   Success: {success_count}")
        print(f"   Errors: {error_count}")
        
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