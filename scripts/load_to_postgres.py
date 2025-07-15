import os
import json
import glob
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASS = os.getenv('POSTGRES_PASSWORD')

DATA_DIR = 'data/raw/telegram_messages'

# Connect to PostgreSQL
def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def ensure_table():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute('''
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE TABLE IF NOT EXISTS raw.telegram_messages (
                id SERIAL PRIMARY KEY,
                channel TEXT,
                message_id BIGINT,
                message_json JSONB,
                message_date TIMESTAMP
            );
        ''')
        conn.commit()

def load_json_files():
    files = glob.glob(os.path.join(DATA_DIR, '*', '*.json'))
    all_rows = []
    for file in files:
        channel = os.path.splitext(os.path.basename(file))[0]
        with open(file, 'r', encoding='utf-8') as f:
            messages = json.load(f)
            for msg in messages:
                msg_id = msg.get('id')
                msg_date = msg.get('date')
                all_rows.append((channel, msg_id, json.dumps(msg), msg_date))
    return all_rows

def insert_rows(rows):
    with get_conn() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            '''INSERT INTO raw.telegram_messages (channel, message_id, message_json, message_date)
               VALUES %s
               ON CONFLICT DO NOTHING''',
            rows
        )
        conn.commit()

def main():
    ensure_table()
    rows = load_json_files()
    if rows:
        insert_rows(rows)
        print(f"Inserted {len(rows)} messages into raw.telegram_messages.")
    else:
        print("No messages found to load.")

if __name__ == '__main__':
    main() 