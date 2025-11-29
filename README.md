# Job Scraper

Fetch jobs hourly from multiple APIs and RSS feeds, extract skills, seniority, and emails, and save them to MongoDB.


## Setup

1. Setup & Running:

```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```


2. Install dependencies:

```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```


3. CronJob example

```bash
0 * * * * /usr/bin/python3 /full/path/to/jobs_import.py >> /full/path/to/jobs_import.log 2>&1
```


4. Logs
    - All imports and errors are logged to the cron log file:

```text
                  ┌───────────────┐
                  │  jobs_import  │
                  │   script      │
                  └──────┬────────┘
                         │
         ┌───────────────┴───────────────────┐
         │                                   │        
         ▼                                   ▼
 ┌───────────────┐                   ┌────────────────┐
 │  Fetch APIs   │                   │   Fetch RSS    │
 │(Adzuna,Jooble,│                   │(RemoteOK,etc.) │
 │ Remotive)     │                   └──────────────-─┘
 └───────┬───────┘
         │
         ▼
 ┌───────────────┐
 │ Process Jobs  │
 │ - Parse date  │
 │ - Extract     │
 │   skills      │
 │ - Extract     │
 │   seniority   │
 │ - Extract     │
 │   email       │
 └───────┬───────┘
         │
         ▼
 ┌───────────────┐
 │  Upsert into  │
 │   MongoDB     │
 │ - jobs        │
 │ - companies   │
 └───────────────┘
