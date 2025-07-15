from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List

# Top products (most mentioned)
def get_top_products(db: Session, limit: int = 10):
    sql = text('''
        SELECT message_text, COUNT(*) as count
        FROM fct_messages
        GROUP BY message_text
        ORDER BY count DESC
        LIMIT :limit
    ''')
    return db.execute(sql, {'limit': limit}).fetchall()

# Channel activity (daily/weekly)
def get_channel_activity(db: Session, channel: str):
    daily_sql = text('''
        SELECT date_id, COUNT(*) as daily_posts
        FROM fct_messages
        WHERE channel_id = :channel
        GROUP BY date_id
        ORDER BY date_id
    ''')
    weekly_sql = text('''
        SELECT DATE_TRUNC('week', date_id) as week, COUNT(*) as weekly_posts
        FROM fct_messages
        WHERE channel_id = :channel
        GROUP BY week
        ORDER BY week
    ''')
    daily = db.execute(daily_sql, {'channel': channel}).fetchall()
    weekly = db.execute(weekly_sql, {'channel': channel}).fetchall()
    return daily, weekly

# Search messages
def search_messages(db: Session, query: str):
    sql = text('''
        SELECT message_id, channel_id, message_text, message_date
        FROM fct_messages
        WHERE message_text ILIKE :query
        ORDER BY message_date DESC
        LIMIT 50
    ''')
    return db.execute(sql, {'query': f'%{query}%'}).fetchall() 