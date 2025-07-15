from pydantic import BaseModel
from typing import List, Optional

class ProductReport(BaseModel):
    product: str
    count: int

class ChannelActivity(BaseModel):
    channel: str
    daily_posts: List[dict]
    weekly_posts: List[dict]

class MessageSearchResult(BaseModel):
    message_id: int
    channel: str
    message_text: str
    message_date: str 