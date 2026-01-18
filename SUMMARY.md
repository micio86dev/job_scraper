# Job Scraper - IT Job Hub

The IT Job Hub scraper is a focused Python-based automation tool that ensures the platform's job board is always fresh and high-quality.

## Workflow

1.  **Orchestration**: Manages sequential scraping from multiple providers with a configurable date lookback window.
2.  **AI Enrichment**: Uses OpenAI (GPT-4o-mini) to categorize job titles and extract required skills.
3.  **Geocoding**: Validates company locations and fetches GPS coordinates via Google Maps.
4.  **Deduplication**: Prevents importing existing jobs by verifying links and critical data.
5.  **Data Persistence**: Clean and standardized insertion into MongoDB.

## Data Sources

- **APIs**: **LinkedIn** (via Apify, executed first), Adzuna, Jooble, RemoteOK, Arbeitnow.
- **Web/RSS**: JobisJob, WeWorkRemotely, Himalayas.app.

## Maintenance Tools

- **fix_dates.py**: Standardizes MongoDB date formats for seamless Prisma integration.
- **Deduplication Engine**: Intelligent filtering based on URL and content analysis.
- **Comprehensive Logging**: Tracking AI token usage and run status in `job_scraper.log`.
- **Date Window Filtering**: Ensures only recent jobs (e.g., last 2 days) are imported to maintain platform freshness.
