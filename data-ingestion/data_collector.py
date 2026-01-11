# import requests
# import pandas as pd
# import os
# import json
# import time
# import logging
# from datetime import datetime, timedelta
# from typing import List, Dict, Optional, Any
# import sqlalchemy
# from sqlalchemy import create_engine, text
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.exc import SQLAlchemyError

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger(__name__)

# class EODDataCollector:
#     def __init__(self, api_key: str, db_url: str = None):
#         self.api_key = api_key
#         self.base_url = "https://api.eoddata.com"
#         self.session = requests.Session()
#         self.session.headers.update({
#             "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
#             "Accept": "application/json"
#         })
        
#         # Initialize database connection if provided
#         if db_url:
#             self.engine = create_engine(db_url)
#             self.SessionLocal = sessionmaker(bind=self.engine)
#         else:
#             self.engine = None
#             self.SessionLocal = None
        
#         # Rate limiting
#         self.last_request_time = 0
#         self.request_delay = 2.0  # 2 seconds between requests
#         self.max_retries = 3
    
#     def _rate_limit(self):
#         """Simple rate limiting"""
#         time_since_last = time.time() - self.last_request_time
#         if time_since_last < self.request_delay:
#             time.sleep(self.request_delay - time_since_last)
#         self.last_request_time = time.time()
    
#     def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
#         """Make authenticated API request"""
#         url = f"{self.base_url}{endpoint}"
        
#         if params is None:
#             params = {}
        
#         # Note: API key parameter is 'apiKey' (lowercase 'k')
#         params['apiKey'] = self.api_key
        
#         self._rate_limit()
        
#         for attempt in range(self.max_retries):
#             try:
#                 logger.debug(f"Requesting: {url}")
                
#                 response = self.session.get(
#                     url, 
#                     params=params,
#                     timeout=30
#                 )
                
#                 if response.status_code == 200:
#                     try:
#                         data = response.json()
#                         logger.debug(f"Response received, length: {len(str(data))}")
#                         return data
#                     except json.JSONDecodeError:
#                         logger.error(f"Invalid JSON response from {endpoint}")
#                         logger.error(f"Response text: {response.text[:500]}")
#                         return {}
#                 elif response.status_code == 401:
#                     logger.error(f"Unauthorized - Check API key")
#                     return {}
#                 elif response.status_code == 429:
#                     retry_after = 30
#                     logger.warning(f"Rate limit exceeded. Waiting {retry_after} seconds...")
#                     time.sleep(retry_after)
#                     continue
#                 else:
#                     logger.error(f"API error {response.status_code}: {response.text[:200]}")
#                     if attempt < self.max_retries - 1:
#                         time.sleep(2 ** attempt)  # Exponential backoff
#                     continue
                    
#             except requests.exceptions.RequestException as e:
#                 logger.error(f"Request failed (attempt {attempt + 1}): {str(e)}")
#                 if attempt < self.max_retries - 1:
#                     time.sleep(5)
#                     continue
        
#         return {}
    
#     def get_exchanges(self) -> List[Dict[str, str]]:
#         """Get ALL available exchanges"""
#         logger.info("Fetching ALL exchanges list...")
        
#         # According to docs: /exchange/list
#         endpoint = "/exchange/list"
#         data = self._make_request(endpoint)
        
#         if isinstance(data, list):
#             all_exchanges = []
#             for ex in data:
#                 if isinstance(ex, dict):
#                     exchange_info = {
#                         'code': ex.get('code', ''),
#                         'name': ex.get('name', ''),
#                         'country': ex.get('country', ''),
#                         'currency': ex.get('currency', ''),
#                         'timeZone': ex.get('timeZone', '')
#                     }
#                     # Only add if it has a code
#                     if exchange_info['code']:
#                         all_exchanges.append(exchange_info)
            
#             logger.info(f"Found {len(all_exchanges)} total exchanges")
#             return all_exchanges
        
#         logger.warning(f"No exchanges returned from API. Data: {data}")
#         return []
    
#     def get_symbols(self, exchange_code: str, limit: int = 20) -> List[Dict[str, Any]]:
#         """Get symbols for an exchange"""
#         logger.info(f"Fetching symbols for {exchange_code}...")
        
#         # According to docs: /symbol/list/{exchangeCode}
#         endpoint = f"/symbol/list/{exchange_code}"
#         data = self._make_request(endpoint)
        
#         if isinstance(data, list):
#             symbols = []
#             count = 0
            
#             for symbol in data:
#                 if count >= limit:
#                     break
                    
#                 if isinstance(symbol, dict):
#                     symbol_code = symbol.get('code', '')
#                     symbol_name = symbol.get('name', '')
                    
#                     # Skip if no code or name
#                     if not symbol_code or not symbol_name:
#                         continue
                    
#                     # Get symbol data
#                     symbol_data = {
#                         'code': symbol_code,
#                         'name': symbol_name,
#                         'exchange': exchange_code,
#                         'type': symbol.get('type', ''),
#                         'currency': symbol.get('currency', ''),
#                         'open': symbol.get('open'),
#                         'close': symbol.get('close'),
#                         'volume': symbol.get('volume')
#                     }
                    
#                     # Add to list
#                     symbols.append(symbol_data)
#                     count += 1
            
#             logger.info(f"Found {len(symbols)} symbols for {exchange_code}")
#             return symbols
        
#         logger.warning(f"No symbols returned for {exchange_code}. Data: {data}")
#         return []
    
#     def get_historical_data(
#         self, 
#         symbol: str, 
#         exchange: str,
#         from_date: str = None,
#         to_date: str = None,
#         interval: str = "d",
#         limit_days: int = 30
#     ) -> pd.DataFrame:
#         """Get historical OHLCV data"""
#         logger.info(f"Fetching {interval} data for {symbol}.{exchange}")
        
#         # According to docs: /quote/list/{exchangeCode}/{symbolCode}
#         endpoint = f"/quote/list/{exchange}/{symbol}"
        
#         params = {
#             'interval': interval,  # 'd' for daily
#         }
        
#         # Set date range for 30 days
#         if not from_date:
#             from_date = (datetime.now() - timedelta(days=limit_days)).strftime('%Y-%m-%d')
#         if not to_date:
#             to_date = datetime.now().strftime('%Y-%m-%d')
        
#         params['from'] = from_date
#         params['to'] = to_date
        
#         data = self._make_request(endpoint, params)
        
#         if isinstance(data, list) and len(data) > 0:
#             df = pd.DataFrame(data)
            
#             # Debug logging
#             logger.debug(f"Raw columns: {df.columns.tolist()}")
            
#             # Standardize column names based on actual response
#             column_mapping = {}
#             for col in df.columns:
#                 col_lower = str(col).lower()
#                 if 'date' in col_lower:
#                     column_mapping[col] = 'date'
#                 elif col_lower == 'open':
#                     column_mapping[col] = 'open'
#                 elif col_lower == 'high':
#                     column_mapping[col] = 'high'
#                 elif col_lower == 'low':
#                     column_mapping[col] = 'low'
#                 elif col_lower == 'close':
#                     column_mapping[col] = 'close'
#                 elif col_lower == 'volume':
#                     column_mapping[col] = 'volume'
#                 elif 'adjusted' in col_lower:
#                     column_mapping[col] = 'adjusted_close'
            
#             df = df.rename(columns=column_mapping)
            
#             # Ensure required columns exist
#             if 'date' not in df.columns and len(df.columns) > 0:
#                 # Try to find date column by data type
#                 for col in df.columns:
#                     if 'stamp' in str(col).lower() or 'time' in str(col).lower():
#                         df = df.rename(columns={col: 'date'})
#                         break
            
#             if 'date' not in df.columns:
#                 logger.warning(f"No date column found for {symbol}")
#                 return pd.DataFrame()
            
#             # Convert date column
#             try:
#                 df['date'] = pd.to_datetime(df['date'])
#             except:
#                 logger.warning(f"Could not parse date column for {symbol}")
#                 return pd.DataFrame()
            
#             # Add metadata
#             df['symbol'] = symbol
#             df['exchange'] = exchange
#             df['interval'] = interval
#             df['download_timestamp'] = datetime.now()
            
#             # Convert numeric columns
#             numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'adjusted_close']
#             for col in numeric_cols:
#                 if col in df.columns:
#                     df[col] = pd.to_numeric(df[col], errors='coerce')
            
#             # Sort by date
#             df = df.sort_values('date').reset_index(drop=True)
            
#             # Limit to approximately 30 days if more data returned
#             if len(df) > limit_days:
#                 df = df.tail(limit_days)
            
#             logger.info(f"Retrieved {len(df)} records for {symbol}")
#             return df
        
#         else:
#             logger.warning(f"No historical data returned for {symbol}.{exchange}")
#             # Try to get today's data as fallback
#             return self._get_today_data(symbol, exchange)
    
#     def _get_today_data(self, symbol: str, exchange: str) -> pd.DataFrame:
#         """Fallback: get today's data"""
#         try:
#             # According to docs: /quote/get/{exchangeCode}/{symbolCode}
#             endpoint = f"/quote/get/{exchange}/{symbol}"
            
#             data = self._make_request(endpoint)
            
#             if isinstance(data, dict) and data:
#                 today = datetime.now().strftime('%Y-%m-%d')
#                 df = pd.DataFrame([{
#                     'date': today,
#                     'open': data.get('open'),
#                     'high': data.get('high'),
#                     'low': data.get('low'),
#                     'close': data.get('close'),
#                     'volume': data.get('volume'),
#                     'symbol': symbol,
#                     'exchange': exchange,
#                     'interval': 'd',
#                     'download_timestamp': datetime.now()
#                 }])
                
#                 df['date'] = pd.to_datetime(df['date'])
#                 for col in ['open', 'high', 'low', 'close', 'volume']:
#                     df[col] = pd.to_numeric(df[col], errors='coerce')
                
#                 logger.info(f"Got today's data for {symbol}")
#                 return df
            
#         except Exception as e:
#             logger.error(f"Failed to get today's data: {e}")
        
#         return pd.DataFrame()
    
#     def save_to_database(self, df: pd.DataFrame, table_name: str = "stock_prices") -> bool:
#         """Save data to PostgreSQL database"""
#         if df.empty or not self.engine:
#             logger.warning("No data or no database connection")
#             return False
        
#         try:
#             db_session = self.SessionLocal()
#             saved_count = 0
            
#             for _, row in df.iterrows():
#                 try:
#                     # Check if record exists
#                     query = text(f"""
#                         SELECT 1 FROM {table_name} 
#                         WHERE symbol = :symbol 
#                         AND date = :date 
#                         AND exchange = :exchange
#                         LIMIT 1
#                     """)
                    
#                     exists = db_session.execute(query, {
#                         'symbol': str(row['symbol']),
#                         'date': row['date'].date(),
#                         'exchange': str(row['exchange'])
#                     }).fetchone()
                    
#                     if not exists:
#                         # Prepare insert statement
#                         columns = ['symbol', 'date', 'exchange']
#                         values = {
#                             'symbol': str(row['symbol']),
#                             'date': row['date'].date(),
#                             'exchange': str(row['exchange'])
#                         }
                        
#                         # Add optional columns
#                         optional_cols = ['open', 'high', 'low', 'close', 'volume']
#                         for col in optional_cols:
#                             if col in row and pd.notna(row[col]):
#                                 columns.append(col)
#                                 if col == 'volume':
#                                     values[col] = int(row[col]) if pd.notna(row[col]) else None
#                                 else:
#                                     values[col] = float(row[col]) if pd.notna(row[col]) else None
                        
#                         # Build dynamic insert
#                         insert_stmt = text(f"""
#                             INSERT INTO {table_name} ({', '.join(columns)})
#                             VALUES ({', '.join([':' + col for col in columns])})
#                         """)
                        
#                         db_session.execute(insert_stmt, values)
#                         saved_count += 1
                    
#                 except Exception as e:
#                     logger.error(f"Error saving row: {e}")
#                     continue
            
#             db_session.commit()
#             db_session.close()
            
#             logger.info(f"Saved {saved_count} records to database")
#             return saved_count > 0
            
#         except SQLAlchemyError as e:
#             logger.error(f"Database error: {str(e)}")
#             return False
#         except Exception as e:
#             logger.error(f"Error saving to database: {str(e)}")
#             return False

# class DataManager:
#     def __init__(self, collector, base_dir: str = "/data"):
#         self.collector = collector
#         self.base_dir = base_dir
        
#         # Create directories
#         self._create_directories()
    
#     def _create_directories(self):
#         """Create necessary directories"""
#         directories = [
#             self.base_dir,
#             os.path.join(self.base_dir, "raw"),
#             os.path.join(self.base_dir, "processed"),
#             os.path.join(self.base_dir, "logs"),
#             os.path.join(self.base_dir, "reports")
#         ]
        
#         for directory in directories:
#             os.makedirs(directory, exist_ok=True)
    
#     def save_data_csv(self, df: pd.DataFrame, symbol: str, exchange: str) -> Optional[str]:
#         """Save data to CSV file"""
#         if df.empty:
#             return None
        
#         try:
#             # Create directory structure
#             date_str = datetime.now().strftime('%Y%m%d')
#             dir_path = os.path.join(
#                 self.base_dir,
#                 "raw",
#                 exchange,
#                 date_str
#             )
            
#             os.makedirs(dir_path, exist_ok=True)
            
#             # CSV file path
#             csv_file = os.path.join(dir_path, f"{symbol}.csv")
            
#             # Save to CSV
#             df.to_csv(csv_file, index=False)
            
#             # Log info
#             file_size = os.path.getsize(csv_file)
#             logger.info(f"✓ Saved {symbol}.{exchange} to {csv_file} ({file_size:,} bytes)")
            
#             return csv_file
            
#         except Exception as e:
#             logger.error(f"✗ Failed to save {symbol}: {str(e)}")
#             return None
    
#     def collect_exchange_data(self, exchange_code: str, num_symbols: int = 20, 
#                              days_of_data: int = 30) -> Dict[str, Any]:
#         """Collect data for multiple symbols from one exchange"""
#         logger.info(f"\n{'='*60}")
#         logger.info(f"COLLECTING FROM {exchange_code}")
#         logger.info(f"Config: {num_symbols} symbols, {days_of_data} days of data")
#         logger.info(f"{'='*60}")
        
#         results = {
#             'exchange': exchange_code,
#             'successful': [],
#             'failed': [],
#             'files': [],
#             'database_success': 0,
#             'database_failed': 0,
#             'total_data_points': 0,
#             'start_time': datetime.now().isoformat()
#         }
        
#         # Get symbols for this exchange
#         symbols = self.collector.get_symbols(exchange_code, limit=num_symbols)
        
#         if not symbols:
#             logger.error(f"No symbols found for {exchange_code}")
#             results['end_time'] = datetime.now().isoformat()
#             return results
        
#         logger.info(f"Processing {len(symbols)} symbols...")
        
#         for i, symbol_data in enumerate(symbols):
#             symbol_code = symbol_data.get('code', '')
#             symbol_name = symbol_data.get('name', 'N/A')
            
#             logger.info(f"\n[{i+1}/{len(symbols)}] {symbol_code}: {symbol_name}")
            
#             try:
#                 # Get historical data (30 days)
#                 df = self.collector.get_historical_data(
#                     symbol=symbol_code,
#                     exchange=exchange_code,
#                     limit_days=days_of_data
#                 )
                
#                 if not df.empty:
#                     # Save to CSV
#                     csv_file = self.save_data_csv(df, symbol_code, exchange_code)
                    
#                     # Save to database if connected
#                     db_success = False
#                     if hasattr(self.collector, 'engine') and self.collector.engine:
#                         db_success = self.collector.save_to_database(df)
                    
#                     if csv_file:
#                         results['successful'].append(symbol_code)
#                         results['files'].append(csv_file)
#                         results['total_data_points'] += len(df)
                        
#                         if db_success:
#                             results['database_success'] += 1
#                         else:
#                             results['database_failed'] += 1
                        
#                         logger.info(f"  ✓ {len(df)} records")
#                         if len(df) > 0:
#                             logger.info(f"    Date range: {df['date'].min().date()} to {df['date'].max().date()}")
#                     else:
#                         results['failed'].append(symbol_code)
#                         logger.error(f"  ✗ Failed to save CSV")
#                 else:
#                     results['failed'].append(symbol_code)
#                     logger.warning(f"  ✗ No data retrieved")
                    
#             except Exception as e:
#                 logger.error(f"Error processing {symbol_code}: {str(e)}")
#                 results['failed'].append(symbol_code)
            
#             # Rate limiting between symbols
#             if i < len(symbols) - 1:
#                 time.sleep(2)  # 2 seconds between symbols
        
#         results['end_time'] = datetime.now().isoformat()
#         return results
    
#     def collect_all_exchanges(self, num_symbols_per_exchange: int = 20, 
#                             days_of_data: int = 30, 
#                             max_exchanges: int = None) -> Dict[str, Any]:
#         """Collect data from ALL exchanges"""
#         all_results = {
#             'total_successful': 0,
#             'total_failed': 0,
#             'total_data_points': 0,
#             'exchanges': {},
#             'start_time': datetime.now().isoformat()
#         }
        
#         # Get ALL exchanges
#         logger.info("Getting ALL exchanges...")
#         exchanges = self.collector.get_exchanges()
        
#         if not exchanges:
#             logger.error("No exchanges found via API")
#             return all_results
        
#         logger.info(f"Found {len(exchanges)} exchanges")
        
#         # List of exchanges to process (all or limited by max_exchanges)
#         target_exchanges = exchanges
#         if max_exchanges:
#             target_exchanges = exchanges[:max_exchanges]
        
#         logger.info(f"Processing {len(target_exchanges)} exchanges...")
        
#         for idx, exchange in enumerate(target_exchanges):
#             exchange_code = exchange['code']
#             exchange_name = exchange['name']
            
#             logger.info(f"\n{'='*60}")
#             logger.info(f"STARTING COLLECTION FOR {exchange_code} ({exchange_name})")
#             logger.info(f"Progress: {idx+1}/{len(target_exchanges)}")
#             logger.info(f"{'='*60}")
            
#             try:
#                 results = self.collect_exchange_data(
#                     exchange_code=exchange_code,
#                     num_symbols=num_symbols_per_exchange,
#                     days_of_data=days_of_data
#                 )
                
#                 all_results['exchanges'][exchange_code] = results
#                 all_results['total_successful'] += len(results['successful'])
#                 all_results['total_failed'] += len(results['failed'])
#                 all_results['total_data_points'] += results['total_data_points']
                
#                 # Summary for this exchange
#                 logger.info(f"\n✓ {exchange_code} completed:")
#                 logger.info(f"  Successful symbols: {len(results['successful'])}")
#                 logger.info(f"  Failed symbols: {len(results['failed'])}")
#                 logger.info(f"  Data points: {results['total_data_points']}")
                
#             except Exception as e:
#                 logger.error(f"Error processing exchange {exchange_code}: {e}")
#                 # Add empty results for failed exchange
#                 all_results['exchanges'][exchange_code] = {
#                     'exchange': exchange_code,
#                     'successful': [],
#                     'failed': [],
#                     'files': [],
#                     'database_success': 0,
#                     'database_failed': 0,
#                     'total_data_points': 0,
#                     'start_time': datetime.now().isoformat(),
#                     'end_time': datetime.now().isoformat(),
#                     'error': str(e)
#                 }
            
#             # Wait between exchanges (except for the last one)
#             if idx < len(target_exchanges) - 1:
#                 wait_time = 5  # 5 seconds between exchanges
#                 logger.info(f"\nWaiting {wait_time}s before next exchange...")
#                 time.sleep(wait_time)
        
#         all_results['end_time'] = datetime.now().isoformat()
#         return all_results

# def main():
#     """Main collection function"""
#     import os
    
#     # Read configuration from environment variables
#     api_key = os.getenv('EODDATA_API_KEY')
#     db_url = os.getenv('DATABASE_URL')
#     data_dir = os.getenv('LOCAL_DATA_DIR', '/data')
#     symbols_per_exchange = int(os.getenv('SYMBOLS_PER_EXCHANGE', '20'))
#     days_of_data = int(os.getenv('DAYS_OF_DATA', '30'))
#     max_exchanges = os.getenv('MAX_EXCHANGES')
#     if max_exchanges:
#         max_exchanges = int(max_exchanges)
    
#     logger.info("=" * 60)
#     logger.info("EODData COLLECTOR - STARTING (ALL EXCHANGES)")
#     logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
#     logger.info(f"Config: {symbols_per_exchange} symbols/exchange, {days_of_data} days of data")
#     if max_exchanges:
#         logger.info(f"Max exchanges: {max_exchanges}")
#     logger.info("=" * 60)
    
#     # Validate configuration
#     if not api_key:
#         logger.error("EODDATA_API_KEY is not set")
#         logger.info("Please set the environment variable EODDATA_API_KEY")
#         return None
    
#     logger.info(f"Using API key: {api_key[:8]}...")
    
#     # Initialize collector
#     collector = EODDataCollector(api_key=api_key, db_url=db_url)
#     manager = DataManager(collector, base_dir=data_dir)
    
#     # Test API connection first
#     logger.info("Testing API connection...")
#     test_exchanges = collector.get_exchanges()
#     if not test_exchanges:
#         logger.error("API connection test failed or no exchanges returned")
#         return None
    
#     logger.info(f"API connection successful. Found {len(test_exchanges)} exchanges")
    
#     # Collect data from ALL exchanges
#     all_results = manager.collect_all_exchanges(
#         num_symbols_per_exchange=symbols_per_exchange,
#         days_of_data=days_of_data,
#         max_exchanges=max_exchanges
#     )
    
#     # Print summary
#     print_summary(all_results)
    
#     # Save report
#     save_report(all_results, data_dir)
    
#     return all_results

# def print_summary(all_results: Dict[str, Any]):
#     """Print collection summary"""
#     logger.info("\n" + "=" * 60)
#     logger.info("COLLECTION SUMMARY (ALL EXCHANGES)")
#     logger.info("=" * 60)
    
#     total_successful = all_results['total_successful']
#     total_failed = all_results['total_failed']
#     total_exchanges = len(all_results['exchanges'])
    
#     logger.info(f"Total exchanges processed: {total_exchanges}")
#     logger.info(f"Total successful symbols: {total_successful}")
#     logger.info(f"Total failed symbols: {total_failed}")
#     logger.info(f"Total data points: {all_results['total_data_points']}")
    
#     total_attempted = total_successful + total_failed
#     if total_attempted > 0:
#         success_rate = (total_successful / total_attempted) * 100
#         logger.info(f"Success rate: {success_rate:.1f}%")
    
#     # Top 10 exchanges by data points
#     logger.info("\nTop 10 exchanges by data collected:")
#     exchange_stats = []
#     for exchange_code, results in all_results['exchanges'].items():
#         if results['total_data_points'] > 0:
#             exchange_stats.append({
#                 'exchange': exchange_code,
#                 'symbols': len(results['successful']),
#                 'data_points': results['total_data_points']
#             })
    
#     # Sort by data points (descending)
#     exchange_stats.sort(key=lambda x: x['data_points'], reverse=True)
    
#     for i, stat in enumerate(exchange_stats[:10]):
#         logger.info(f"  {i+1}. {stat['exchange']}: {stat['symbols']} symbols, {stat['data_points']} data points")
    
#     # Exchanges with no data
#     no_data_exchanges = []
#     for exchange_code, results in all_results['exchanges'].items():
#         if results['total_data_points'] == 0:
#             no_data_exchanges.append(exchange_code)
    
#     if no_data_exchanges:
#         logger.info(f"\nExchanges with no data ({len(no_data_exchanges)}):")
#         logger.info(f"  {', '.join(no_data_exchanges[:10])}")
#         if len(no_data_exchanges) > 10:
#             logger.info(f"  ... and {len(no_data_exchanges) - 10} more")

# def save_report(all_results: Dict[str, Any], base_dir: str = "/data"):
#     """Save collection report to file"""
#     report_dir = os.path.join(base_dir, "reports")
#     os.makedirs(report_dir, exist_ok=True)
    
#     report_file = os.path.join(
#         report_dir, 
#         f"collection_report_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
#     )
    
#     try:
#         with open(report_file, 'w') as f:
#             json.dump(all_results, f, indent=2, default=str)
#         logger.info(f"\n✓ Detailed report saved to: {report_file}")
        
#         # Also save a simple CSV summary
#         csv_file = report_file.replace('.json', '.csv')
#         summary_data = []
        
#         for exchange_code, results in all_results['exchanges'].items():
#             summary_data.append({
#                 'exchange': exchange_code,
#                 'successful_symbols': len(results['successful']),
#                 'failed_symbols': len(results['failed']),
#                 'data_points': results['total_data_points'],
#                 'files': len(results['files']),
#                 'database_success': results['database_success'],
#                 'database_failed': results['database_failed']
#             })
        
#         if summary_data:
#             df = pd.DataFrame(summary_data)
#             df.to_csv(csv_file, index=False)
#             logger.info(f"✓ CSV summary saved to: {csv_file}")
            
#     except Exception as e:
#         logger.error(f"Failed to save report: {e}")

# # Test function
# def test_api():
#     """Test API endpoints"""
#     import os
    
#     api_key = os.getenv('EODDATA_API_KEY')
#     if not api_key:
#         print("Please set EODDATA_API_KEY environment variable")
#         return
    
#     collector = EODDataCollector(api_key=api_key)
    
#     print("Testing EODData API endpoints...")
    
#     # Test 1: Exchange list
#     print("\n1. Testing /exchange/list...")
#     exchanges = collector.get_exchanges()
#     print(f"   Found {len(exchanges)} exchanges")
#     for ex in exchanges[:5]:
#         print(f"   - {ex['code']}: {ex['name']} ({ex['country']})")
    
#     # Test 2: Symbols for first exchange
#     if exchanges:
#         exchange_code = exchanges[0]['code']
#         print(f"\n2. Testing /symbol/list/{exchange_code}...")
#         symbols = collector.get_symbols(exchange_code, limit=5)
#         print(f"   Found {len(symbols)} symbols")
#         for sym in symbols[:3]:
#             print(f"   - {sym['code']}: {sym['name']}")
    
#     # Test 3: Historical data for first symbol
#     if symbols:
#         symbol = symbols[0]['code']
#         print(f"\n3. Testing /quote/list/{exchange_code}/{symbol}...")
#         df = collector.get_historical_data(
#             symbol=symbol,
#             exchange=exchange_code,
#             limit_days=10
#         )
#         print(f"   Retrieved {len(df)} records")
#         if not df.empty:
#             print(f"   Columns: {df.columns.tolist()}")
#             print(f"   Date range: {df['date'].min()} to {df['date'].max()}")

# if __name__ == "__main__":
#     # Check if we're in test mode
#     test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
    
#     if test_mode:
#         test_api()
#     else:
#         main()

import requests
import pandas as pd
import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EODDataCollector:
    def __init__(self, api_key: str, db_url: str = None):
        self.api_key = api_key
        self.base_url = "https://api.eoddata.com"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json"
        })
        
        # Initialize database connection if provided
        if db_url:
            self.engine = create_engine(db_url)
            self.SessionLocal = sessionmaker(bind=self.engine)
        else:
            self.engine = None
            self.SessionLocal = None
        
        # Rate limiting
        self.last_request_time = 0
        self.request_delay = 2.0  # 2 seconds between requests
        self.max_retries = 3
    
    def _rate_limit(self):
        """Simple rate limiting"""
        time_since_last = time.time() - self.last_request_time
        if time_since_last < self.request_delay:
            time.sleep(self.request_delay - time_since_last)
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make authenticated API request"""
        url = f"{self.base_url}{endpoint}"
        
        if params is None:
            params = {}
        
        # Note: API key parameter is 'apiKey' (lowercase 'k')
        params['apiKey'] = self.api_key
        
        self._rate_limit()
        
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"Requesting: {url}")
                
                response = self.session.get(
                    url, 
                    params=params,
                    timeout=30
                )
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        logger.debug(f"Response received, length: {len(str(data))}")
                        return data
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON response from {endpoint}")
                        logger.error(f"Response text: {response.text[:500]}")
                        return {}
                elif response.status_code == 401:
                    logger.error(f"Unauthorized - Check API key")
                    return {}
                elif response.status_code == 429:
                    retry_after = 30
                    logger.warning(f"Rate limit exceeded. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                else:
                    logger.error(f"API error {response.status_code}: {response.text[:200]}")
                    if attempt < self.max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}): {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(5)
                    continue
        
        return {}
    
    def get_exchanges(self) -> List[Dict[str, str]]:
        """Get ALL available exchanges"""
        logger.info("Fetching ALL exchanges list...")
        
        # According to docs: /exchange/list
        endpoint = "/exchange/list"
        data = self._make_request(endpoint)
        
        if isinstance(data, list):
            all_exchanges = []
            for ex in data:
                if isinstance(ex, dict):
                    exchange_info = {
                        'code': ex.get('code', ''),
                        'name': ex.get('name', ''),
                        'country': ex.get('country', ''),
                        'currency': ex.get('currency', ''),
                        'timeZone': ex.get('timeZone', '')
                    }
                    # Only add if it has a code
                    if exchange_info['code']:
                        all_exchanges.append(exchange_info)
            
            logger.info(f"Found {len(all_exchanges)} total exchanges")
            return all_exchanges
        
        logger.warning(f"No exchanges returned from API. Data: {data}")
        return []
    
    def get_symbols(self, exchange_code: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Get symbols for an exchange"""
        logger.info(f"Fetching symbols for {exchange_code}...")
        
        # According to docs: /symbol/list/{exchangeCode}
        endpoint = f"/symbol/list/{exchange_code}"
        data = self._make_request(endpoint)
        
        if isinstance(data, list):
            symbols = []
            count = 0
            
            for symbol in data:
                if count >= limit:
                    break
                    
                if isinstance(symbol, dict):
                    symbol_code = symbol.get('code', '')
                    symbol_name = symbol.get('name', '')
                    
                    # Skip if no code or name
                    if not symbol_code or not symbol_name:
                        continue
                    
                    # Get symbol data
                    symbol_data = {
                        'code': symbol_code,
                        'name': symbol_name,
                        'exchange': exchange_code,
                        'type': symbol.get('type', ''),
                        'currency': symbol.get('currency', ''),
                        'open': symbol.get('open'),
                        'close': symbol.get('close'),
                        'volume': symbol.get('volume')
                    }
                    
                    # Add to list
                    symbols.append(symbol_data)
                    count += 1
            
            logger.info(f"Found {len(symbols)} symbols for {exchange_code}")
            return symbols
        
        logger.warning(f"No symbols returned for {exchange_code}. Data: {data}")
        return []
    
    def get_historical_data(
        self, 
        symbol: str, 
        exchange: str,
        from_date: str = None,
        to_date: str = None,
        interval: str = "d",
        limit_days: int = 30
    ) -> pd.DataFrame:
        """Get historical OHLCV data"""
        logger.info(f"Fetching {interval} data for {symbol}.{exchange}")
        
        # According to docs: /quote/list/{exchangeCode}/{symbolCode}
        endpoint = f"/quote/list/{exchange}/{symbol}"
        
        params = {
            'interval': interval,  # 'd' for daily
        }
        
        # Set date range for 30 days
        if not from_date:
            from_date = (datetime.now() - timedelta(days=limit_days)).strftime('%Y-%m-%d')
        if not to_date:
            to_date = datetime.now().strftime('%Y-%m-%d')
        
        params['from'] = from_date
        params['to'] = to_date
        
        data = self._make_request(endpoint, params)
        
        if isinstance(data, list) and len(data) > 0:
            df = pd.DataFrame(data)
            
            # Debug logging
            logger.debug(f"Raw columns: {df.columns.tolist()}")
            
            # Standardize column names based on actual response
            column_mapping = {}
            for col in df.columns:
                col_lower = str(col).lower()
                if 'date' in col_lower:
                    column_mapping[col] = 'date'
                elif col_lower == 'open':
                    column_mapping[col] = 'open'
                elif col_lower == 'high':
                    column_mapping[col] = 'high'
                elif col_lower == 'low':
                    column_mapping[col] = 'low'
                elif col_lower == 'close':
                    column_mapping[col] = 'close'
                elif col_lower == 'volume':
                    column_mapping[col] = 'volume'
                elif 'adjusted' in col_lower:
                    column_mapping[col] = 'adjusted_close'
            
            df = df.rename(columns=column_mapping)
            
            # Ensure required columns exist
            if 'date' not in df.columns and len(df.columns) > 0:
                # Try to find date column by data type
                for col in df.columns:
                    if 'stamp' in str(col).lower() or 'time' in str(col).lower():
                        df = df.rename(columns={col: 'date'})
                        break
            
            if 'date' not in df.columns:
                logger.warning(f"No date column found for {symbol}")
                return pd.DataFrame()
            
            # Convert date column
            try:
                df['date'] = pd.to_datetime(df['date'])
            except:
                logger.warning(f"Could not parse date column for {symbol}")
                return pd.DataFrame()
            
            # Add metadata
            df['symbol'] = symbol
            df['exchange'] = exchange
            df['interval'] = interval
            df['download_timestamp'] = datetime.now()
            
            # Convert numeric columns
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'adjusted_close']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Sort by date
            df = df.sort_values('date').reset_index(drop=True)
            
            # Limit to approximately 30 days if more data returned
            if len(df) > limit_days:
                df = df.tail(limit_days)
            
            logger.info(f"Retrieved {len(df)} records for {symbol}")
            return df
        
        else:
            logger.warning(f"No historical data returned for {symbol}.{exchange}")
            # Try to get today's data as fallback
            return self._get_today_data(symbol, exchange)
    
    def _get_today_data(self, symbol: str, exchange: str) -> pd.DataFrame:
        """Fallback: get today's data"""
        try:
            # According to docs: /quote/get/{exchangeCode}/{symbolCode}
            endpoint = f"/quote/get/{exchange}/{symbol}"
            
            data = self._make_request(endpoint)
            
            if isinstance(data, dict) and data:
                today = datetime.now().strftime('%Y-%m-%d')
                df = pd.DataFrame([{
                    'date': today,
                    'open': data.get('open'),
                    'high': data.get('high'),
                    'low': data.get('low'),
                    'close': data.get('close'),
                    'volume': data.get('volume'),
                    'symbol': symbol,
                    'exchange': exchange,
                    'interval': 'd',
                    'download_timestamp': datetime.now()
                }])
                
                df['date'] = pd.to_datetime(df['date'])
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                logger.info(f"Got today's data for {symbol}")
                return df
            
        except Exception as e:
            logger.error(f"Failed to get today's data: {e}")
        
        return pd.DataFrame()
    
    def save_to_database(self, df: pd.DataFrame, table_name: str = "stock_prices") -> bool:
        """Save data to PostgreSQL database"""
        if df.empty or not self.engine:
            logger.warning("No data or no database connection")
            return False
        
        try:
            db_session = self.SessionLocal()
            saved_count = 0
            
            for _, row in df.iterrows():
                try:
                    # Check if record exists
                    query = text(f"""
                        SELECT 1 FROM {table_name} 
                        WHERE symbol = :symbol 
                        AND date = :date 
                        AND exchange = :exchange
                        LIMIT 1
                    """)
                    
                    exists = db_session.execute(query, {
                        'symbol': str(row['symbol']),
                        'date': row['date'].date(),
                        'exchange': str(row['exchange'])
                    }).fetchone()
                    
                    if not exists:
                        # Prepare insert statement
                        columns = ['symbol', 'date', 'exchange']
                        values = {
                            'symbol': str(row['symbol']),
                            'date': row['date'].date(),
                            'exchange': str(row['exchange'])
                        }
                        
                        # Add optional columns
                        optional_cols = ['open', 'high', 'low', 'close', 'volume']
                        for col in optional_cols:
                            if col in row and pd.notna(row[col]):
                                columns.append(col)
                                if col == 'volume':
                                    values[col] = int(row[col]) if pd.notna(row[col]) else None
                                else:
                                    values[col] = float(row[col]) if pd.notna(row[col]) else None
                        
                        # Build dynamic insert
                        insert_stmt = text(f"""
                            INSERT INTO {table_name} ({', '.join(columns)})
                            VALUES ({', '.join([':' + col for col in columns])})
                        """)
                        
                        db_session.execute(insert_stmt, values)
                        saved_count += 1
                    
                except Exception as e:
                    logger.error(f"Error saving row: {e}")
                    continue
            
            db_session.commit()
            db_session.close()
            
            logger.info(f"Saved {saved_count} records to database")
            return saved_count > 0
            
        except SQLAlchemyError as e:
            logger.error(f"Database error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error saving to database: {str(e)}")
            return False

class DataManager:
    def __init__(self, collector, base_dir: str = "/data"):
        self.collector = collector
        self.base_dir = base_dir
        
        # Create directories
        self._create_directories()
    
    def _create_directories(self):
        """Create necessary directories"""
        directories = [
            self.base_dir,
            os.path.join(self.base_dir, "raw"),
            os.path.join(self.base_dir, "processed"),
            os.path.join(self.base_dir, "logs"),
            os.path.join(self.base_dir, "reports")
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def save_data_csv(self, df: pd.DataFrame, symbol: str, exchange: str) -> Optional[str]:
        """Save data to CSV file"""
        if df.empty:
            return None
        
        try:
            # Create directory structure
            date_str = datetime.now().strftime('%Y%m%d')
            dir_path = os.path.join(
                self.base_dir,
                "raw",
                exchange,
                date_str
            )
            
            os.makedirs(dir_path, exist_ok=True)
            
            # CSV file path
            csv_file = os.path.join(dir_path, f"{symbol}.csv")
            
            # Save to CSV
            df.to_csv(csv_file, index=False)
            
            # Log info
            file_size = os.path.getsize(csv_file)
            logger.info(f"✓ Saved {symbol}.{exchange} to {csv_file} ({file_size:,} bytes)")
            
            return csv_file
            
        except Exception as e:
            logger.error(f"✗ Failed to save {symbol}: {str(e)}")
            return None
    
    def upload_exchange_to_hdfs(self, exchange_code: str):
        """Upload exchange data to HDFS immediately after collection"""
        try:
            from hdfs_uploader import upload_exchange_files
            
            logger.info(f"📤 Uploading {exchange_code} data to HDFS...")
            upload_count = upload_exchange_files(exchange_code)
            
            if upload_count > 0:
                logger.info(f"✅ Successfully uploaded {upload_count} files for {exchange_code} to HDFS")
                return upload_count
            else:
                logger.warning(f"⚠️ No files uploaded for {exchange_code} to HDFS")
                return 0
                
        except Exception as e:
            logger.error(f"❌ Failed to upload {exchange_code} to HDFS: {e}")
            return 0
    
    def collect_and_upload_exchange(self, exchange_code: str, num_symbols: int = 20, 
                                   days_of_data: int = 30) -> Dict[str, Any]:
        """Collect data for exchange and upload to HDFS immediately"""
        logger.info(f"\n{'='*60}")
        logger.info(f"COLLECTING FROM {exchange_code}")
        logger.info(f"Config: {num_symbols} symbols, {days_of_data} days of data")
        logger.info(f"{'='*60}")
        
        results = {
            'exchange': exchange_code,
            'successful': [],
            'failed': [],
            'files': [],
            'database_success': 0,
            'database_failed': 0,
            'total_data_points': 0,
            'hdfs_upload_count': 0,
            'start_time': datetime.now().isoformat()
        }
        
        # Get symbols for this exchange
        symbols = self.collector.get_symbols(exchange_code, limit=num_symbols)
        
        if not symbols:
            logger.error(f"No symbols found for {exchange_code}")
            results['end_time'] = datetime.now().isoformat()
            return results
        
        logger.info(f"Processing {len(symbols)} symbols...")
        
        for i, symbol_data in enumerate(symbols):
            symbol_code = symbol_data.get('code', '')
            symbol_name = symbol_data.get('name', 'N/A')
            
            logger.info(f"\n[{i+1}/{len(symbols)}] {symbol_code}: {symbol_name}")
            
            try:
                # Get historical data
                df = self.collector.get_historical_data(
                    symbol=symbol_code,
                    exchange=exchange_code,
                    limit_days=days_of_data
                )
                
                if not df.empty:
                    # Save to CSV
                    csv_file = self.save_data_csv(df, symbol_code, exchange_code)
                    
                    # Save to database if connected
                    db_success = False
                    if hasattr(self.collector, 'engine') and self.collector.engine:
                        db_success = self.collector.save_to_database(df)
                    
                    if csv_file:
                        results['successful'].append(symbol_code)
                        results['files'].append(csv_file)
                        results['total_data_points'] += len(df)
                        
                        if db_success:
                            results['database_success'] += 1
                        else:
                            results['database_failed'] += 1
                        
                        logger.info(f"  ✓ {len(df)} records")
                        if len(df) > 0:
                            logger.info(f"    Date range: {df['date'].min().date()} to {df['date'].max().date()}")
                    else:
                        results['failed'].append(symbol_code)
                        logger.error(f"  ✗ Failed to save CSV")
                else:
                    results['failed'].append(symbol_code)
                    logger.warning(f"  ✗ No data retrieved")
                    
            except Exception as e:
                logger.error(f"Error processing {symbol_code}: {str(e)}")
                results['failed'].append(symbol_code)
            
            # Rate limiting between symbols
            if i < len(symbols) - 1:
                time.sleep(2)  # 2 seconds between symbols
        
        # UPLOAD EXCHANGE DATA TO HDFS IMMEDIATELY
        if results['successful']:
            logger.info(f"\n📤 Uploading {exchange_code} data to HDFS...")
            hdfs_upload_count = self.upload_exchange_to_hdfs(exchange_code)
            results['hdfs_upload_count'] = hdfs_upload_count
            logger.info(f"✓ {exchange_code} uploaded to HDFS: {hdfs_upload_count} files")
        else:
            logger.info(f"⚠️ No successful symbols for {exchange_code}, skipping HDFS upload")
        
        results['end_time'] = datetime.now().isoformat()
        
        # Print exchange summary
        logger.info(f"\n{'='*40}")
        logger.info(f"EXCHANGE SUMMARY: {exchange_code}")
        logger.info(f"{'='*40}")
        logger.info(f"Successful symbols: {len(results['successful'])}")
        logger.info(f"Failed symbols: {len(results['failed'])}")
        logger.info(f"Data points: {results['total_data_points']}")
        logger.info(f"Files uploaded to HDFS: {results['hdfs_upload_count']}")
        logger.info(f"Database success: {results['database_success']}")
        logger.info(f"Database failed: {results['database_failed']}")
        
        return results
    
    def collect_all_exchanges(self, num_symbols_per_exchange: int = 20, 
                            days_of_data: int = 30, 
                            max_exchanges: int = None) -> Dict[str, Any]:
        """Collect data from ALL exchanges and upload each immediately"""
        all_results = {
            'total_successful': 0,
            'total_failed': 0,
            'total_data_points': 0,
            'total_hdfs_uploads': 0,
            'exchanges': {},
            'start_time': datetime.now().isoformat()
        }
        
        # Get ALL exchanges
        logger.info("Getting ALL exchanges...")
        exchanges = self.collector.get_exchanges()
        
        if not exchanges:
            logger.error("No exchanges found via API")
            return all_results
        
        logger.info(f"Found {len(exchanges)} exchanges")
        
        # List of exchanges to process (all or limited by max_exchanges)
        target_exchanges = exchanges
        if max_exchanges:
            target_exchanges = exchanges[:max_exchanges]
        
        logger.info(f"Processing {len(target_exchanges)} exchanges...")
        
        for idx, exchange in enumerate(target_exchanges):
            exchange_code = exchange['code']
            exchange_name = exchange['name']
            
            logger.info(f"\n{'='*60}")
            logger.info(f"STARTING COLLECTION FOR {exchange_code} ({exchange_name})")
            logger.info(f"Progress: {idx+1}/{len(target_exchanges)}")
            logger.info(f"{'='*60}")
            
            try:
                # Collect and upload exchange data immediately
                results = self.collect_and_upload_exchange(
                    exchange_code=exchange_code,
                    num_symbols=num_symbols_per_exchange,
                    days_of_data=days_of_data
                )
                
                all_results['exchanges'][exchange_code] = results
                all_results['total_successful'] += len(results['successful'])
                all_results['total_failed'] += len(results['failed'])
                all_results['total_data_points'] += results['total_data_points']
                all_results['total_hdfs_uploads'] += results['hdfs_upload_count']
                
                # Summary for this exchange
                logger.info(f"\n✓ {exchange_code} completed and uploaded to HDFS:")
                logger.info(f"  Successful symbols: {len(results['successful'])}")
                logger.info(f"  Failed symbols: {len(results['failed'])}")
                logger.info(f"  Data points: {results['total_data_points']}")
                logger.info(f"  HDFS uploads: {results['hdfs_upload_count']}")
                
            except Exception as e:
                logger.error(f"Error processing exchange {exchange_code}: {e}")
                # Add empty results for failed exchange
                all_results['exchanges'][exchange_code] = {
                    'exchange': exchange_code,
                    'successful': [],
                    'failed': [],
                    'files': [],
                    'database_success': 0,
                    'database_failed': 0,
                    'total_data_points': 0,
                    'hdfs_upload_count': 0,
                    'start_time': datetime.now().isoformat(),
                    'end_time': datetime.now().isoformat(),
                    'error': str(e)
                }
            
            # Wait between exchanges (except for the last one)
            if idx < len(target_exchanges) - 1:
                wait_time = 5  # 5 seconds between exchanges
                logger.info(f"\nWaiting {wait_time}s before next exchange...")
                time.sleep(wait_time)
        
        all_results['end_time'] = datetime.now().isoformat()
        return all_results

def main():
    """Main collection function"""
    import os
    
    # Read configuration from environment variables
    api_key = os.getenv('EODDATA_API_KEY')
    db_url = os.getenv('DATABASE_URL')
    data_dir = os.getenv('LOCAL_DATA_DIR', '/data')
    symbols_per_exchange = int(os.getenv('SYMBOLS_PER_EXCHANGE', '20'))
    days_of_data = int(os.getenv('DAYS_OF_DATA', '30'))
    max_exchanges = os.getenv('MAX_EXCHANGES')
    if max_exchanges:
        max_exchanges = int(max_exchanges)
    
    logger.info("=" * 60)
    logger.info("EODData COLLECTOR - STARTING (ALL EXCHANGES)")
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Config: {symbols_per_exchange} symbols/exchange, {days_of_data} days of data")
    logger.info("NOTE: Each exchange will be uploaded to HDFS immediately after collection")
    if max_exchanges:
        logger.info(f"Max exchanges: {max_exchanges}")
    logger.info("=" * 60)
    
    # Validate configuration
    if not api_key:
        logger.error("EODDATA_API_KEY is not set")
        logger.info("Please set the environment variable EODDATA_API_KEY")
        return None
    
    logger.info(f"Using API key: {api_key[:8]}...")
    
    # Initialize collector
    collector = EODDataCollector(api_key=api_key, db_url=db_url)
    manager = DataManager(collector, base_dir=data_dir)
    
    # Test API connection first
    logger.info("Testing API connection...")
    test_exchanges = collector.get_exchanges()
    if not test_exchanges:
        logger.error("API connection test failed or no exchanges returned")
        return None
    
    logger.info(f"API connection successful. Found {len(test_exchanges)} exchanges")
    
    # Collect data from ALL exchanges with immediate HDFS upload
    all_results = manager.collect_all_exchanges(
        num_symbols_per_exchange=symbols_per_exchange,
        days_of_data=days_of_data,
        max_exchanges=max_exchanges
    )
    
    # Print summary
    print_summary(all_results)
    
    # Save report
    save_report(all_results, data_dir)
    
    return all_results

def print_summary(all_results: Dict[str, Any]):
    """Print collection summary"""
    logger.info("\n" + "=" * 60)
    logger.info("COLLECTION SUMMARY (ALL EXCHANGES)")
    logger.info("=" * 60)
    
    total_successful = all_results['total_successful']
    total_failed = all_results['total_failed']
    total_exchanges = len(all_results['exchanges'])
    
    logger.info(f"Total exchanges processed: {total_exchanges}")
    logger.info(f"Total successful symbols: {total_successful}")
    logger.info(f"Total failed symbols: {total_failed}")
    logger.info(f"Total data points: {all_results['total_data_points']}")
    logger.info(f"Total HDFS uploads: {all_results['total_hdfs_uploads']}")
    
    total_attempted = total_successful + total_failed
    if total_attempted > 0:
        success_rate = (total_successful / total_attempted) * 100
        logger.info(f"Success rate: {success_rate:.1f}%")
    
    # Top 10 exchanges by data points
    logger.info("\nTop 10 exchanges by data collected:")
    exchange_stats = []
    for exchange_code, results in all_results['exchanges'].items():
        if results['total_data_points'] > 0:
            exchange_stats.append({
                'exchange': exchange_code,
                'symbols': len(results['successful']),
                'data_points': results['total_data_points'],
                'hdfs_uploads': results['hdfs_upload_count']
            })
    
    # Sort by data points (descending)
    exchange_stats.sort(key=lambda x: x['data_points'], reverse=True)
    
    for i, stat in enumerate(exchange_stats[:10]):
        logger.info(f"  {i+1}. {stat['exchange']}: {stat['symbols']} symbols, "
                   f"{stat['data_points']} data points, "
                   f"{stat['hdfs_uploads']} HDFS files")
    
    # Exchanges with no data
    no_data_exchanges = []
    for exchange_code, results in all_results['exchanges'].items():
        if results['total_data_points'] == 0:
            no_data_exchanges.append(exchange_code)
    
    if no_data_exchanges:
        logger.info(f"\nExchanges with no data ({len(no_data_exchanges)}):")
        logger.info(f"  {', '.join(no_data_exchanges[:10])}")
        if len(no_data_exchanges) > 10:
            logger.info(f"  ... and {len(no_data_exchanges) - 10} more")

def save_report(all_results: Dict[str, Any], base_dir: str = "/data"):
    """Save collection report to file"""
    report_dir = os.path.join(base_dir, "reports")
    os.makedirs(report_dir, exist_ok=True)
    
    report_file = os.path.join(
        report_dir, 
        f"collection_report_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    
    try:
        with open(report_file, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)
        logger.info(f"\n✓ Detailed report saved to: {report_file}")
        
        # Also save a simple CSV summary
        csv_file = report_file.replace('.json', '.csv')
        summary_data = []
        
        for exchange_code, results in all_results['exchanges'].items():
            summary_data.append({
                'exchange': exchange_code,
                'successful_symbols': len(results['successful']),
                'failed_symbols': len(results['failed']),
                'data_points': results['total_data_points'],
                'hdfs_upload_count': results['hdfs_upload_count'],
                'files': len(results['files']),
                'database_success': results['database_success'],
                'database_failed': results['database_failed']
            })
        
        if summary_data:
            df = pd.DataFrame(summary_data)
            df.to_csv(csv_file, index=False)
            logger.info(f"✓ CSV summary saved to: {csv_file}")
            
    except Exception as e:
        logger.error(f"Failed to save report: {e}")

if __name__ == "__main__":
    main()