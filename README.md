# Ethio Medical Insights Data Platform

## Project Overview
This project builds a robust data platform to generate insights about Ethiopian medical businesses using data scraped from public Telegram channels. The pipeline extracts, loads, and transforms raw data into a clean, analytics-ready warehouse using modern ELT practices.

## Architecture
```
Raw Telegram Channels -> Data Lake (JSON) -> PostgreSQL (raw) -> dbt (star schema) -> Analytics
```

## Setup Instructions

### 1. Clone the repository
```bash
git clone <repo-url>
cd ethio-medical-insights
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Set up environment variables
- Copy `.env.example` to `.env` and fill in your secrets.

### 4. Run Docker (PostgreSQL)
```bash
docker-compose up -d
```

### 5. Scrape Telegram Data
```bash
python scripts/telegram_scraper.py
```

### 6. Load Data into PostgreSQL
```bash
python scripts/load_to_postgres.py
```

### 7. (If dbt is available) Initialize and run dbt
```bash
# In Python 3.10+ environment
pip install dbt-postgres
cd dbt_medical_insights
# Edit profiles.yml with your DB credentials
# Run dbt models
 dbt run
 dbt test
 dbt docs generate
```

## Data Lake Structure
- `data/raw/telegram_messages/YYYY-MM-DD/channel_name.json`
- Images: `data/raw/telegram_messages/YYYY-MM-DD/channel_name/images/`

## Testing
- Check `logs/scraper.log` for scraping status.
- Check PostgreSQL for `raw.telegram_messages` table.
- dbt tests validate data quality (if dbt is set up).

## Contact
For questions, contact the author.