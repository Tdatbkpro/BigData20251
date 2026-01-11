-- ============================================
-- INITIALIZATION SCRIPT FOR STOCK DATABASE
-- ============================================

-- Drop existing tables if needed (careful in production!)
-- DROP TABLE IF EXISTS analysis_results CASCADE;
-- DROP TABLE IF EXISTS stock_prices CASCADE;
-- DROP TABLE IF EXISTS stock_metadata CASCADE;
-- DROP TABLE IF EXISTS exchanges CASCADE;

-- Create tables
CREATE TABLE IF NOT EXISTS stock_metadata (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    exchange VARCHAR(10) NOT NULL,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, exchange)
);

CREATE TABLE IF NOT EXISTS stock_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(10, 4),
    high DECIMAL(10, 4),
    low DECIMAL(10, 4),
    close DECIMAL(10, 4),
    volume BIGINT,
    exchange VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, date, exchange)
);

CREATE TABLE IF NOT EXISTS analysis_results (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    analysis_type VARCHAR(50) NOT NULL,
    result TEXT,
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS exchanges (
    code VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    years INT,
    last_update DATE
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_date ON stock_prices(symbol, date);
CREATE INDEX IF NOT EXISTS idx_stock_prices_date ON stock_prices(date);
CREATE INDEX IF NOT EXISTS idx_stock_metadata_symbol ON stock_metadata(symbol);

-- ============================================
-- INSERT EXCHANGES DATA
-- ============================================
INSERT INTO exchanges (code, name, years, last_update) VALUES
('AMEX','American Stock Exchange',36,'2026-01-02'),
('AMS','Euronext - Amsterdam',36,'2026-01-02'),
('ASX','Australian Securities Exchange',36,'2026-01-02'),
('BRU','Euronext - Brussels',36,'2026-01-02'),
('CC','Cryptocurrencies',17,'2026-01-02'),
('CFE','Chicago Futures Exchange',36,'2026-01-02'),
('EUREX','EUREX Futures Exchange',25,'2026-01-02'),
('FOREX','Foreign Exchange',36,'2026-01-02'),
('FRA','Frankfurt Exchange',36,'2026-01-02'),
('INDEX','Global Indices',97,'2026-01-02'),
('LIFFE','LIFFE Futures and Options',26,'2026-01-02'),
('LIS','Euronext - Lisbon',31,'2026-01-02'),
('LSE','London Stock Exchange',56,'2026-01-02'),
('MGEX','Minneapolis Grain Exchange',36,'2026-01-02'),
('MSE','Madrid Stock Exchange',36,'2026-01-02'),
('NASDAQ','NASDAQ Stock Exchange',46,'2026-01-02'),
('NSE','National Stock Exchange of India',31,'2026-01-02'),
('NYBOT','New York Board of Trade',36,'2026-01-02'),
('NYSE','New York Stock Exchange',46,'2026-01-02'),
('OSL','Euronext - Oslo',36,'2026-01-02'),
('OTCBB','OTC Bulletin Board',38,'2026-01-02'),
('PAR','Euronext - Paris',36,'2026-01-02'),
('SGX','Singapore Stock Exchange',36,'2026-01-02'),
('SHE','Shenzhen Stock Exchange',30,'2025-12-31'),
('SHG','Shanghai Stock Exchange',21,'2025-12-31'),
('TSX','Toronto Stock Exchange',36,'2026-01-02'),
('TSXV','Toronto Venture Exchange',36,'2026-01-02'),
('USMF','Mutual Funds',33,'2025-12-31'),
('WCE','Winnipeg Commodity Exchange',36,'2026-01-02')
ON CONFLICT (code) DO NOTHING;

-- ============================================
-- CREATE TRIGGER FUNCTION
-- ============================================
CREATE OR REPLACE FUNCTION sync_stock_metadata()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO stock_metadata (symbol, exchange)
    VALUES (NEW.symbol, NEW.exchange)
    ON CONFLICT (symbol, exchange) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- CLEAN AND INSERT DATA
-- ============================================

-- First, disable or drop existing trigger
DROP TRIGGER IF EXISTS trg_stock_prices_to_metadata ON stock_prices;

-- Clear existing data (optional - careful!)
-- TRUNCATE TABLE analysis_results CASCADE;
-- TRUNCATE TABLE stock_prices CASCADE;
-- TRUNCATE TABLE stock_metadata CASCADE;

-- Insert stock metadata
DO $$
BEGIN
    RAISE NOTICE 'Inserting stock metadata...';
    
    INSERT INTO stock_metadata (symbol, exchange, company_name, sector, industry)
    VALUES 
        ('AAPL', 'NASDAQ', 'Apple Inc.', 'Technology', 'Consumer Electronics'),
        ('MSFT', 'NASDAQ', 'Microsoft Corporation', 'Technology', 'Software'),
        ('GOOGL', 'NASDAQ', 'Alphabet Inc.', 'Technology', 'Internet Content & Information'),
        ('AMZN', 'NASDAQ', 'Amazon.com Inc.', 'Consumer Cyclical', 'Internet Retail'),
        ('TSLA', 'NASDAQ', 'Tesla Inc.', 'Consumer Cyclical', 'Auto Manufacturers'),
        ('JPM', 'NYSE', 'JPMorgan Chase & Co.', 'Financial Services', 'Banks'),
        ('V', 'NYSE', 'Visa Inc.', 'Financial Services', 'Credit Services'),
        ('WMT', 'NYSE', 'Walmart Inc.', 'Consumer Defensive', 'Discount Stores'),
        ('JNJ', 'NYSE', 'Johnson & Johnson', 'Healthcare', 'Drug Manufacturers'),
        ('PG', 'NYSE', 'Procter & Gamble Co.', 'Consumer Defensive', 'Household & Personal Products'),
        ('NVDA', 'NASDAQ', 'NVIDIA Corporation', 'Technology', 'Semiconductors'),
        ('META', 'NASDAQ', 'Meta Platforms Inc.', 'Technology', 'Internet Content & Information'),
        ('NFLX', 'NASDAQ', 'Netflix Inc.', 'Communication Services', 'Entertainment'),
        ('DIS', 'NYSE', 'The Walt Disney Company', 'Communication Services', 'Entertainment'),
        ('BAC', 'NYSE', 'Bank of America Corp', 'Financial Services', 'Banks'),
        ('XOM', 'NYSE', 'Exxon Mobil Corporation', 'Energy', 'Oil & Gas Integrated'),
        ('CVX', 'NYSE', 'Chevron Corporation', 'Energy', 'Oil & Gas Integrated'),
        ('HD', 'NYSE', 'Home Depot Inc.', 'Consumer Cyclical', 'Home Improvement Retail'),
        ('MA', 'NYSE', 'Mastercard Incorporated', 'Financial Services', 'Credit Services')
    ON CONFLICT (symbol, exchange) DO NOTHING;
    
    RAISE NOTICE 'Stock metadata inserted: % rows', (SELECT COUNT(*) FROM stock_metadata);
END $$;

-- Insert stock prices
DO $$
BEGIN
    RAISE NOTICE 'Inserting stock prices...';
    
    INSERT INTO stock_prices (symbol, date, open, high, low, close, volume, exchange)
    WITH RECURSIVE dates AS (
        SELECT CURRENT_DATE - 30 AS date
        UNION ALL
        SELECT date + 1
        FROM dates
        WHERE date < CURRENT_DATE
    ),
    stock_data AS (
        SELECT 
            sm.symbol,
            dates.date,
            ROUND((50 + RANDOM() * 200)::numeric, 2) AS base_price,
            sm.exchange
        FROM stock_metadata sm
        CROSS JOIN dates
        ORDER BY sm.symbol, dates.date
    )
    SELECT 
        symbol,
        date,
        base_price AS open,
        ROUND((base_price + RANDOM() * 10)::numeric, 2) AS high,
        ROUND((base_price - RANDOM() * 10)::numeric, 2) AS low,
        ROUND((base_price + (RANDOM() - 0.5) * 5)::numeric, 2) AS close,
        (1000000 + RANDOM() * 9000000)::bigint AS volume,
        exchange
    FROM stock_data
    ON CONFLICT (symbol, date, exchange) DO NOTHING;
    
    RAISE NOTICE 'Stock prices inserted: % rows', (SELECT COUNT(*) FROM stock_prices);
END $$;

-- Insert analysis results
DO $$
BEGIN
    RAISE NOTICE 'Inserting analysis results...';
    
    INSERT INTO analysis_results (symbol, analysis_type, result, generated_at)
    VALUES
        ('AAPL', 'technical_analysis', '{"rsi": 65.2, "macd": 2.5, "sma_50": 175.3, "sma_200": 168.7, "support": 170.0, "resistance": 185.0, "signal": "BUY", "confidence": 0.75}', CURRENT_DATE - 1),
        ('MSFT', 'technical_analysis', '{"rsi": 58.7, "macd": 1.8, "sma_50": 320.5, "sma_200": 305.2, "support": 315.0, "resistance": 335.0, "signal": "HOLD", "confidence": 0.68}', CURRENT_DATE - 1),
        ('GOOGL', 'volatility_analysis', '{"volatility_30d": 0.25, "beta": 1.05, "sharpe_ratio": 1.8, "max_drawdown": -0.12, "var_95": -0.08, "risk_score": 0.35}', CURRENT_DATE - 2),
        ('AMZN', 'momentum_analysis', '{"momentum_10d": 0.08, "momentum_30d": 0.15, "relative_strength": 72.5, "trend": "bullish", "trend_strength": 0.82}', CURRENT_DATE - 2),
        ('TSLA', 'sentiment_analysis', '{"news_sentiment": 0.65, "social_mentions": 12500, "sentiment_score": 0.72, "confidence": 0.85, "bullish_percent": 0.68}', CURRENT_DATE - 1),
        ('NVDA', 'technical_analysis', '{"rsi": 70.3, "macd": 15.2, "sma_50": 480.5, "sma_200": 420.3, "support": 475.0, "resistance": 520.0, "signal": "STRONG_BUY", "confidence": 0.88}', CURRENT_DATE),
        ('JPM', 'fundamental_analysis', '{"pe_ratio": 10.5, "pb_ratio": 1.4, "roe": 0.15, "dividend_yield": 0.025, "eps_growth": 0.12, "recommendation": "BUY"}', CURRENT_DATE - 3),
        ('AAPL', 'volume_analysis', '{"avg_volume_10d": 45000000, "volume_ratio": 1.2, "volume_trend": "increasing", "unusual_volume": false, "volume_signal": "NORMAL"}', CURRENT_DATE),
        ('MSFT', 'correlation_analysis', '{"corr_with_nasdaq": 0.85, "corr_with_aapl": 0.72, "sector_correlation": 0.68, "market_beta": 0.95, "diversification_score": 0.42}', CURRENT_DATE - 1),
        ('GOOGL', 'risk_analysis', '{"var_95": -0.08, "cvar_95": -0.12, "max_loss_30d": -0.15, "risk_score": 0.35, "risk_level": "MODERATE"}', CURRENT_DATE - 2)
    ON CONFLICT DO NOTHING;
    
    RAISE NOTICE 'Analysis results inserted: % rows', (SELECT COUNT(*) FROM analysis_results);
END $$;

-- ============================================
-- CREATE TRIGGER (after data insertion)
-- ============================================
CREATE TRIGGER trg_stock_prices_to_metadata
AFTER INSERT ON stock_prices
FOR EACH ROW
EXECUTE FUNCTION sync_stock_metadata();

-- ============================================
-- VERIFICATION QUERIES
-- ============================================
DO $$
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'DATABASE INITIALIZATION COMPLETE';
    RAISE NOTICE '============================================';
    RAISE NOTICE 'stock_metadata: % rows', (SELECT COUNT(*) FROM stock_metadata);
    RAISE NOTICE 'stock_prices: % rows', (SELECT COUNT(*) FROM stock_prices);
    RAISE NOTICE 'analysis_results: % rows', (SELECT COUNT(*) FROM analysis_results);
    RAISE NOTICE 'exchanges: % rows', (SELECT COUNT(*) FROM exchanges);
    RAISE NOTICE '============================================';
END $$;