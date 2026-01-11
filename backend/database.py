from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://stockuser:stockpass@postgres:5432/stockdb"
)

# Create engine
engine = create_engine(
    DATABASE_URL,
    pool_size=20,
    max_overflow=30,
    pool_pre_ping=True,
    echo=True  # Đặt True để debug
)

# Create session factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# TẠO Base TRƯỚC
Base = declarative_base()

# QUAN TRỌNG: Import models SAU KHI tạo Base
from models import StockMetadata, StockPrice, AnalysisResult

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()