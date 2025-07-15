import os
import json
import logging
from datetime import datetime
from telethon.sync import TelegramClient
from telethon.tl.types import MessageMediaPhoto
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
SESSION_NAME = 'session'

# Channels to scrape
CHANNELS = [
    'lobelia4cosmetics',
    # Add more channel usernames as needed
]

# Data lake base directory
DATA_LAKE_DIR = 'data/raw/telegram_messages'

# Logging setup
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    filename='logs/scraper.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)


def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def already_scraped(channel, date_str):
    """Check if data for this channel and date already exists."""
    json_path = os.path.join(DATA_LAKE_DIR, date_str, f'{channel}.json')
    return os.path.exists(json_path)


def save_messages(messages, channel, date_str):
    out_dir = os.path.join(DATA_LAKE_DIR, date_str)
    ensure_dir(out_dir)
    json_path = os.path.join(out_dir, f'{channel}.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(messages, f, ensure_ascii=False, indent=2)


def save_image(message, channel, date_str):
    img_dir = os.path.join(DATA_LAKE_DIR, date_str, channel, 'images')
    ensure_dir(img_dir)
    return message.download_media(file=img_dir)


async def scrape_channel(client, channel, limit=100):
    date_str = datetime.now().strftime('%Y-%m-%d')
    if already_scraped(channel, date_str):
        logging.info(f"Skipping {channel} for {date_str} (already scraped)")
        return
    logging.info(f"Scraping {channel} for {date_str}")
    messages = []
    try:
        async for message in client.iter_messages(channel, limit=limit):
            msg_dict = message.to_dict()
            if message.media and isinstance(message.media, MessageMediaPhoto):
                img_path = await save_image(message, channel, date_str)
                msg_dict['downloaded_image'] = img_path
            messages.append(msg_dict)
        save_messages(messages, channel, date_str)
        logging.info(f"Saved {len(messages)} messages for {channel} on {date_str}")
    except Exception as e:
        logging.error(f"Error scraping {channel}: {e}")


def main():
    from telethon import TelegramClient
    import asyncio
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    async def runner():
        await client.start()
        for channel in CHANNELS:
            await scrape_channel(client, channel)
        await client.disconnect()
    asyncio.run(runner())

if __name__ == '__main__':
    main() 