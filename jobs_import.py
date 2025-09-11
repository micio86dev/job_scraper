#!/usr/bin/env python3
import os, re, hashlib, requests, logging, time
from datetime import datetime, timedelta, timezone
from typing import List, Any, Optional
from langdetect import detect, DetectorFactory, LangDetectException
from pymongo import MongoClient, ASCENDING
from dateutil import parser as dtparser
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import spacy

DetectorFactory.seed = 0

# ============ CONFIG ============
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "itjobhub")
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
JOOBLE_KEY = os.getenv("JOOBLE_KEY")

LANG_WHITELIST = {"en", "es", "fr", "de", "it"}
# spaCy IT/EN model (install with: pip install spacy && python -m spacy download en_core_web_sm)
NLP = spacy.load("en_core_web_sm")

TECH_SKILLS = {
    "python",
    "java",
    "javascript",
    "typescript",
    "react",
    "vue",
    "vuejs",
    "angular",
    "node",
    "bun",
    "django",
    "socket",
    "flask",
    "fastapi",
    "firebase",
    "css",
    "html",
    "flutter",
    "reactnative",
    "nativescript",
    "rest",
    "api",
    "c++",
    "c#",
    "c",
    "rust",
    "rocket",
    "qwik",
    "go",
    "ruby",
    "rails",
    "php",
    "laravel",
    "spring",
    "sql",
    "mongodb",
    "terraform",
    "git",
    "mysql",
    "postgres",
    "gcp",
    "redis",
    "docker",
    "kubernetes",
    "aws",
    "gcp",
    "azure",
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

# Mongo setup
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
jobs_col = db["jobs"]
companies_col = db["companies"]
jobs_col.create_index([("link", ASCENDING)], unique=True)


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
            # Se è naive → la consideriamo UTC
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            # Se ha tz → convertiamola a UTC
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


def upsert_company(name: str, raw: dict) -> Any:
    if not name:
        return None
    norm = re.sub(r"\W+", "", name.lower())
    res = companies_col.find_one_and_update(
        {"normalized": norm},
        {
            "$set": {
                "name": name,
                "normalized": norm,
                "raw": raw,
                "updated_at": datetime.now(),
            }
        },
        upsert=True,
        return_document=True,
    )
    return res["_id"]


def save_job(job: dict):
    job["inserted_at"] = datetime.now()
    jobs_col.update_one({"link": job["link"]}, {"$set": job}, upsert=True)


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
    r = requests.get(url, params=params, timeout=30).json()
    out = []
    for j in r.get("results", []):
        out.append(
            {
                "link": j.get("redirect_url"),
                "title": j.get("title"),
                "description": j.get("description"),
                "company": j.get("company", {}).get("display_name"),
                "published": j.get("created"),
                "salary": j.get("salary_max"),
                "raw": j,
            }
        )
    return out


def fetch_jooble() -> List[dict]:
    if not JOOBLE_KEY:
        print("⚠️ Nessuna JOOBLE_KEY trovata, salto Jooble")
        return []

    import json

    try:
        import http.client

        conn = http.client.HTTPConnection("jooble.org")
        headers = {"Content-type": "application/json"}
        body = json.dumps(
            {"keywords": "developer", "location": ""}
        )  # puoi personalizzare location
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
    r = requests.get(
        "https://remotive.com/api/remote-jobs?category=software-dev", timeout=30
    ).json()
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
        out.append(
            {
                "link": it.link.text if it.link else None,  # type: ignore
                "title": it.title.text if it.title else None,  # type: ignore
                "description": it.description.text if it.description else "",  # type: ignore
                "company": None,
                "published": it.pubDate.text if it.pubDate else None,  # type: ignore
                "salary": None,
                "raw": {},
            }
        )
    return out


def fetch_simplyhired(query="developer"):
    url = f"https://www.simplyhired.com/search?q={query}"
    r = requests.get(url, timeout=30)
    soup = BeautifulSoup(r.text, "html.parser")
    out = []

    for job in soup.select("div.SerpJob-jobCard"):
        link_tag = job.select_one("a.SerpJob-link")
        title_tag = job.select_one("a.SerpJob-link span")
        company_tag = job.select_one(".JobPosting-labelWithIcon span")
        date_tag = job.select_one(".JobPosting-labelWithIcon time")
        desc_tag = job.select_one(".JobPosting-snippet")

        link = "https://www.simplyhired.com" + link_tag["href"] if link_tag else None  # type: ignore
        out.append(
            {
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
            }
        )

    return out


# ============ PIPELINE ============
def process_job(raw: dict):
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
        # pattern riserva: cerca numeri romani o parole chiave comuni nel titolo
        title_lower = title.lower()
        if "junior" in title_lower:
            seniority = "junior"
        elif "senior" in title_lower:
            seniority = "senior"
        elif re.search(r"\b(i|ii|iii|iv|v)\b", title_lower):
            seniority = "mid"

    # company
    comp_ref = upsert_company(raw.get("company"), raw.get("raw") or {})  # type: ignore

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
    save_job(job_doc)


def run():
    all_jobs = []
    for country in ["gb", "fr", "de", "it", "es"]:
        all_jobs += fetch_adzuna(country)
    all_jobs += fetch_jooble()
    all_jobs += fetch_remotive()
    all_jobs += fetch_simplyhired("developer")
    all_jobs += fetch_rss(
        "https://weworkremotely.com/categories/remote-programming-jobs.rss"
    )
    all_jobs += fetch_rss("https://remoteok.com/remote-dev-jobs.rss")
    all_jobs += fetch_rss("https://www.python.org/jobs/feed/rss")

    for j in all_jobs:
        try:
            process_job(j)
        except Exception as e:
            print("Error job:", e)


if __name__ == "__main__":
    run()
    print("Import completated.")
