# Job Scraper

Fetch jobs from multiple APIs, scrapers, and RSS feeds, perform AI-based categorization (skills, seniority, location), and save them to MongoDB.

## Features
- **Configurable Date Window**: Standardizes imports to a strict window (default: today and yesterday) to ensure freshness.
- **AI Categorization**: Uses OpenAI to extract structured data (skills, seniority, formatted address) from job descriptions.
- **Geocoding**: Converts company addresses to GPS coordinates using Google Maps API.
- **Deduplication**: Ensures the same job link isn't imported twice.
- **Multi-source**: Fetches from RemoteOK, Arbeitnow, Adzuna, JobisJob, Jooble, and specific technology RSS feeds (WeWorkRemotely, Himalayas).

## Setup

1. **Environment**: Create a `.env` file based on `.env.example` and fill in your API keys (OpenAI, Google Maps, Adzuna, Jooble) and MongoDB URI.

2. **Installation**:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

Run the scraper using `main.py`. You can specify languages, a limit of jobs per language, and the history window.

### Basic command
```bash
python3 main.py
```

### Advanced options
```bash
# Import Italian and English jobs from the last 2 days, limit 10 per language
python3 main.py --languages it,en --limit 10 --days 1
```

**Available Arguments:**
- `--languages`: Comma-separated list of ISO language codes (e.g., `it,en,es`). Defaults to `en,it,es,fr,de`.
- `--limit`: The maximum number of *new* jobs to import for each language.
- `--days`: Lookback window in days (0=today, 1=today/yesterday). Defaults to 1.

## Automation

Set up a CronJob to run the scraper hourly:

```bash
0 * * * * /path/to/venv/bin/python3 /full/path/to/main.py --limit 50 --days 1 >> /full/path/to/job_scraper_cron.log 2>&1
```

## Developer Tools

### Linting
To maintain code quality in Python, we recommend using **flake8** or **black**.
```bash
# Run lint check
flake8 .
```

### Manual Verification
To verify the scraper logic without performing a full run:
1. Ensure your `.env` is correctly configured.
2. Run the main script with a low limit:
   ```bash
   python3 main.py --languages it --limit 1 --days 1
   ```

### Data Integrity Fixes
If you encounter date format issues with Prisma, use the provided utility script:
```bash
python3 fix_dates.py
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
         ┌───────────────┴────────────────────┐
         │                                    │        
         ▼                                    ▼
  ┌───────────────┐                    ┌────────────────┐
  │  Fetch APIs   │                    │   Fetch RSS    │
  │(Adzuna, Jooble│                    │(WeWorkRemotely,│
  │RemoteOK, etc) │                    │ Himalayas)     │
  └───────┬───────┘                    └──────────────-─┘
          │
          ▼
  ┌───────────────┐
  │ Process Jobs  │
  │ - Date Filter │ <--- Strict history window
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
