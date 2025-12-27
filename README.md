# Job Scraper

Fetch jobs from multiple APIs, scrapers, and RSS feeds, perform AI-based categorization (skills, seniority, location), and save them to MongoDB.

## Features
- **Today Only**: Always filters and imports only jobs published on the current date.
- **AI Categorization**: Uses OpenAI to extract structured data (skills, seniority, formatted address) from job descriptions.
- **Geocoding**: Converts company addresses to GPS coordinates using Google Maps API.
- **Deduplication**: Ensures the same job link isn't imported twice.
- **Multi-source**: Fetches from Remotive, Adzuna, JobisJob, InfoJobs, Jooble API, and various RSS feeds.

## Setup

1. **Environment**: Create a `.env` file based on `.env.example` and fill in your API keys (OpenAI, Google Maps, Adzuna) and MongoDB URI.

2. **Installation**:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

Run the scraper using `main.py`. You can specify languages and a limit of jobs per language.

### Basic command
```bash
python3 main.py
```

### Advanced options
```bash
# Import only Italian and English jobs, with a limit of 10 new jobs per language
python3 main.py --languages it,en --limit 10
```

**Available Arguments:**
- `--languages`: Comma-separated list of ISO language codes (e.g., `it,en,es`). Defaults to `en,it,es,fr,de`.
- `--limit`: The maximum number of *new* jobs to import for each language.

## Automation

Set up a CronJob to run the scraper hourly:

```bash
0 * * * * /path/to/venv/bin/python3 /full/path/to/main.py --limit 50 >> /full/path/to/job_scraper_cron.log 2>&1
```

## Logs
Detailed logs are available in `job_scraper.log` for the application logic and `job_scraper_cron.log` for execution status.

## Workflow
```text
                  ┌───────────────┐
                  │  job_scraper  │
                  │   script      │
                  └──────┬────────┘
                         │
         ┌───────────────┴───────────────────┐
         │                                   │        
         ▼                                   ▼
 ┌───────────────┐                   ┌────────────────┐
 │  Fetch APIs   │                   │   Fetch RSS/WEB│
 │(Adzuna,       │                   │(Jooble,        │
 │ Remotive)     │                   │ JobisJob)      │
 └───────┬───────┘                   └──────────────-─┘
         │
         ▼
 ┌───────────────┐
 │ Process Jobs  │
 │ - Date Filter │ <--- Only today's jobs
 │ - Deduplicate │
 │ - AI Enrich   │ (Skills, Seniority, Location)
 │ - Geocode     │
 └───────┬───────┘
         │
         ▼
 ┌───────────────┐
 │  Upsert into  │
 │   MongoDB     │
 │ - jobs        │
 │ - companies   │
 │ - seniorities │
 └───────────────┘
```
