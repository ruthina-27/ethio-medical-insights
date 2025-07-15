# main.py

import dotenv
import os

dotenv.load_dotenv()

from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy import (
    create_engine, Column, Date, String, Float, func
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from typing import List
from datetime import date

# === Database setup ===
DB_URL = "sqlite:///./covid.db"
engine = create_engine(DB_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False)
Base = declarative_base()

class CovidWeekly(Base):
    __tablename__ = "covid_weekly"
    date = Column(Date, primary_key=True)
    country = Column(String, primary_key=True)
    new_cases = Column(Float)

# Ensure table exists (in production, use migrations)
Base.metadata.create_all(bind=engine)

# === Pydantic schema ===
class CovidSummary(BaseModel):
    country: str
    total_cases: float

# === FastAPI app ===
app = FastAPI(title="Ethio Medical Insights Analytical API")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/api/reports/top-products", response_model=List[schemas.ProductReport])
def top_products(limit: int = 10, db: Session = Depends(get_db)):
    results = crud.get_top_products(db, limit)
    return [{"product": r[0], "count": r[1]} for r in results]

@app.get("/api/channels/{channel_name}/activity", response_model=schemas.ChannelActivity)
def channel_activity(channel_name: str, db: Session = Depends(get_db)):
    daily, weekly = crud.get_channel_activity(db, channel_name)
    return schemas.ChannelActivity(
        channel=channel_name,
        daily_posts=[{"date": r[0], "count": r[1]} for r in daily],
        weekly_posts=[{"week": r[0], "count": r[1]} for r in weekly]
    )

@app.get("/api/search/messages", response_model=List[schemas.MessageSearchResult])
def search_messages(query: str = Query(...), db: Session = Depends(get_db)):
    results = crud.search_messages(db, query)
    return [
        schemas.MessageSearchResult(
            message_id=r[0],
            channel=r[1],
            message_text=r[2],
            message_date=str(r[3])
        ) for r in results
    ]
