from pydantic import BaseModel
from datetime import date, datetime
from typing import Optional, List

class StockResponse(BaseModel):
    symbol: str
    exchange: str
    company_name: str
    sector: Optional[str]
    industry: Optional[str]
    
    class Config:
        from_attributes = True

class StockPriceResponse(BaseModel):
    symbol: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    exchange: str
    
    class Config:
        from_attributes = True

class AnalysisResponse(BaseModel):
    symbol: str
    analysis_type: str
    result: str
    generated_at: datetime
    
    class Config:
        from_attributes = True

class HDFSFileResponse(BaseModel):
    path: str
    name: str
    size: int
    type: str
    modification_time: str