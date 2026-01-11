from sqlalchemy import Column, Integer, String, Float, DateTime, Date, Text, BigInteger, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database import Base  # QUAN TRỌNG: import Base từ database.py

class StockMetadata(Base):
    __tablename__ = "stock_metadata"
    
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    exchange = Column(String(10), nullable=False)
    company_name = Column(String(255))
    sector = Column(String(100))
    industry = Column(String(100))
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

class StockPrice(Base):
    __tablename__ = "stock_prices"
    
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    open = Column(Float(precision=4))
    high = Column(Float(precision=4))
    low = Column(Float(precision=4))
    close = Column(Float(precision=4))
    volume = Column(BigInteger)
    exchange = Column(String(10))
    created_at = Column(DateTime, server_default=func.now())

class AnalysisResult(Base):
    __tablename__ = "analysis_results"
    
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    analysis_type = Column(String(50), nullable=False)
    result = Column(Text)
    generated_at = Column(DateTime, server_default=func.now())