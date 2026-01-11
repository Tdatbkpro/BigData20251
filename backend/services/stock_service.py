import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from models import StockPrice, StockMetadata

logger = logging.getLogger(__name__)

class StockService:
    def __init__(self, db: Session):
        self.db = db
    
    def get_analytics_summary(self) -> Dict[str, Any]:
        """Get overall analytics summary"""
        try:
            # Total unique stocks
            total_stocks = self.db.query(func.count(func.distinct(StockPrice.symbol))).scalar() or 0
            
            # Total records
            total_records = self.db.query(func.count(StockPrice.id)).scalar() or 0
            
            # Latest update date
            latest_record = self.db.query(StockPrice).order_by(StockPrice.date.desc()).first()
            
            # Stocks by exchange
            exchange_counts = self.db.query(
                StockPrice.exchange,
                func.count(func.distinct(StockPrice.symbol)).label('count')
            ).group_by(StockPrice.exchange).all()
            
            exchanges = {exchange: count for exchange, count in exchange_counts}
            
            # Date range
            min_date = self.db.query(func.min(StockPrice.date)).scalar()
            max_date = self.db.query(func.max(StockPrice.date)).scalar()
            
            return {
                "total_stocks": total_stocks,
                "total_records": total_records,
                "latest_update": latest_record.date.isoformat() if latest_record else None,
                "exchanges": exchanges,
                "date_range": {
                    "min_date": min_date.isoformat() if min_date else None,
                    "max_date": max_date.isoformat() if max_date else None
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting analytics summary: {str(e)}")
            raise
    
    def get_top_gainers(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get top gaining stocks in last N days"""
        try:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)
            
            # Get first and last prices for each symbol in the period
            subquery = self.db.query(
                StockPrice.symbol,
                StockPrice.exchange,
                func.min(StockPrice.date).label('first_date'),
                func.max(StockPrice.date).label('last_date')
            ).filter(
                StockPrice.date.between(start_date, end_date)
            ).group_by(
                StockPrice.symbol, StockPrice.exchange
            ).subquery()
            
            # Get first and last prices
            first_prices = self.db.query(
                StockPrice.symbol,
                StockPrice.close.label('first_price')
            ).join(
                subquery,
                (StockPrice.symbol == subquery.c.symbol) & 
                (StockPrice.date == subquery.c.first_date)
            ).subquery()
            
            last_prices = self.db.query(
                StockPrice.symbol,
                StockPrice.close.label('last_price'),
                StockPrice.volume
            ).join(
                subquery,
                (StockPrice.symbol == subquery.c.symbol) & 
                (StockPrice.date == subquery.c.last_date)
            ).subquery()
            
            # Calculate percentage change
            result = self.db.query(
                first_prices.c.symbol,
                first_prices.c.first_price,
                last_prices.c.last_price,
                last_prices.c.volume,
                ((last_prices.c.last_price - first_prices.c.first_price) / first_prices.c.first_price * 100).label('change_pct')
            ).join(
                last_prices,
                first_prices.c.symbol == last_prices.c.symbol
            ).order_by(desc('change_pct')).limit(10).all()
            
            return [
                {
                    "symbol": r.symbol,
                    "change": round(r.change_pct, 2),
                    "volume": r.volume,
                    "start_price": r.first_price,
                    "end_price": r.last_price
                }
                for r in result
            ]
            
        except Exception as e:
            logger.error(f"Error getting top gainers: {str(e)}")
            return []
    
    def get_high_volume_stocks(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get stocks with highest average volume in last N days"""
        try:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)
            
            # Get average volume by stock
            volume_stocks = self.db.query(
                StockPrice.symbol,
                StockPrice.exchange,
                func.avg(StockPrice.volume).label('avg_volume')
            ).filter(
                StockPrice.date.between(start_date, end_date)
            ).group_by(
                StockPrice.symbol,
                StockPrice.exchange
            ).order_by(desc('avg_volume')).limit(20).all()
            
            # Get company names
            result = []
            for stock in volume_stocks:
                metadata = self.db.query(StockMetadata).filter(
                    StockMetadata.symbol == stock.symbol
                ).first()
                
                result.append({
                    "symbol": stock.symbol,
                    "exchange": stock.exchange,
                    "company_name": metadata.company_name if metadata else "Unknown",
                    "avg_volume": int(stock.avg_volume) if stock.avg_volume else 0
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting high volume stocks: {str(e)}")
            return []