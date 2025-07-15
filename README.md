# Ethiopian Medical Business Data Platform

A data pipeline for analyzing Ethiopian medical businesses from Telegram channels.

## Features

- Telegram data scraping
- Data lake storage
- PostgreSQL data warehouse
- dbt transformations
- YOLO object detection
- FastAPI analytical API

## Setup

1. Clone the repository
2. Create `.env` file from `.env.example`
3. Run `docker-compose up --build`

## Telegram Scraping

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Set up your `.env` file with your Telegram API credentials.
3. Run the scraper:
   ```bash
   python scripts/telegram_scraper.py
   ```

- Scraped messages will be saved as JSON in `data/raw/telegram_messages/YYYY-MM-DD/channel_name.json`.
- Images will be saved in `data/raw/telegram_messages/YYYY-MM-DD/channel_name/images/`.
- Logs are written to `logs/scraper.log`.