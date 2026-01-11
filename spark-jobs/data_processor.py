from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
import os
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockDataProcessor:
    def __init__(self):
        self.spark = self._create_spark_session()
        self.hdfs_base_path = "hdfs://namenode:9000/stock_data"
    
    def _create_spark_session(self):
        """Create Spark session with HDFS support"""
        return SparkSession.builder \
            .appName("StockDataProcessor") \
            .master("spark://spark-master:7077") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.cores", "1") \
            .config("spark.cores.max", "2") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .getOrCreate()
    
    def read_raw_data(self):
        try:
            path = f"{self.hdfs_base_path}/raw/daily"
            logger.info(f"Reading data from: {path}")

            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(f"{path}/*/*/*.csv")

            df.printSchema()

            count = df.count()
            logger.info(f"Total records read: {count}")

            if count > 0:
                df.show(5, truncate=False)

            return df

        except Exception as e:
            logger.error(f"Failed to read data: {str(e)}")
            return None

    
    def process_data(self, df):
        """Process and clean stock data"""
        if df is None:
            return None
        
        try:
            # Select and rename columns
            processed_df = df.select(
                col("date").cast(DateType()).alias("date"),
                col("open").cast(DoubleType()).alias("open"),
                col("high").cast(DoubleType()).alias("high"),
                col("low").cast(DoubleType()).alias("low"),
                col("close").cast(DoubleType()).alias("close"),
                col("volume").cast(LongType()).alias("volume"),
                col("exchange"),
                col("symbol")
            ).withColumn(
                "download_timestamp", current_timestamp()
            )

            
            # Add calculated columns
            processed_df = processed_df.withColumn(
                "daily_return", 
                ((col("close") - col("open")) / col("open") * 100).cast(DoubleType())
            )
            
            processed_df = processed_df.withColumn(
                "price_range", 
                (col("high") - col("low")).cast(DoubleType())
            )
            
            processed_df = processed_df.withColumn(
                "avg_price", 
                ((col("high") + col("low") + col("close")) / 3).cast(DoubleType())
            )
            
            # Add volume categories
            processed_df = processed_df.withColumn(
                "volume_category",
                when(col("volume") > 1000000, "HIGH")
                .when(col("volume") > 100000, "MEDIUM")
                .otherwise("LOW")
            )
            
            # Filter out invalid records
            processed_df = processed_df.filter(
                (col("close").isNotNull()) &
                (col("volume") > 0) &
                (col("date").isNotNull())
            )
            
            # Add year, month, day columns for partitioning
            processed_df = processed_df.withColumn("year", year(col("date")))
            processed_df = processed_df.withColumn("month", month(col("date")))
            processed_df = processed_df.withColumn("day", dayofmonth(col("date")))
            
            logger.info("Processed data schema:")
            processed_df.printSchema()
            
            return processed_df
            
        except Exception as e:
            logger.error(f"Error processing data: {str(e)}")
            return None
    
    def calculate_aggregations(self, df):
        """Calculate aggregations by symbol"""
        if df is None:
            return None
        
        try:
            # Window specification for calculations
            window_spec = Window.partitionBy("symbol").orderBy("date")
            
            # Calculate moving averages
            df_with_indicators = df.withColumn(
                "ma_5", 
                avg("close").over(window_spec.rowsBetween(-4, 0))
            ).withColumn(
                "ma_20", 
                avg("close").over(window_spec.rowsBetween(-19, 0))
            )
            
            # Calculate daily returns
            df_with_indicators = df_with_indicators.withColumn(
                "prev_close", 
                lag("close", 1).over(window_spec)
            ).withColumn(
                "daily_return_pct",
                when(col("prev_close").isNotNull(),
                     ((col("close") - col("prev_close")) / col("prev_close") * 100)
                ).otherwise(None)
            )
            
            # Aggregations by symbol
            agg_df = df.groupBy("symbol", "exchange") \
                .agg(
                    count("*").alias("record_count"),
                    min("date").alias("first_date"),
                    max("date").alias("last_date"),
                    avg("close").alias("avg_price"),
                    stddev("close").alias("price_volatility"),
                    avg("volume").alias("avg_volume"),
                    max("volume").alias("max_volume"),
                    min("volume").alias("min_volume"),
                    avg("daily_return").alias("avg_daily_return")
                ) \
                .withColumn(
                    "data_span_days", 
                    datediff(col("last_date"), col("first_date"))
                ) \
                .withColumn(
                    "price_range_ratio",
                    col("price_volatility") / col("avg_price") * 100
                )
            
            logger.info("Aggregation results:")
            agg_df.show(10, truncate=False)
            
            return agg_df
            
        except Exception as e:
            logger.error(f"Error calculating aggregations: {str(e)}")
            return None
    
    def save_processed_data(self, df, table_name):
        """Save processed data to HDFS as Parquet"""
        if df is None:
            return False
        
        try:
            output_path = f"{self.hdfs_base_path}/processed/{table_name}"
            
            # Write as Parquet with partitioning
            if "year" in df.columns and "month" in df.columns:
                df.write \
                    .mode("overwrite") \
                    .partitionBy("year", "month") \
                    .parquet(output_path)
            else:
                df.write \
                    .mode("overwrite") \
                    .parquet(output_path)
            
            logger.info(f"Successfully saved data to: {output_path}")
            
            # Also save as CSV for compatibility
            csv_path = f"{self.hdfs_base_path}/processed/{table_name}_csv"
            df.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(csv_path)
            
            logger.info(f"Also saved as CSV to: {csv_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save data: {str(e)}")
            return False
    
    def run_processing_pipeline(self):
        """Run complete processing pipeline"""
        logger.info("=" * 60)
        logger.info("STARTING SPARK PROCESSING PIPELINE")
        logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        try:
            # Step 1: Read raw data
            logger.info("\n[STEP 1] Reading raw data from HDFS...")
            raw_df = self.read_raw_data()
            
            if raw_df is None or raw_df.count() == 0:
                logger.error("No data to process")
                return False
            
            # Step 2: Process data
            logger.info("\n[STEP 2] Processing data...")
            processed_df = self.process_data(raw_df)
            
            if processed_df is None:
                logger.error("Data processing failed")
                return False
            
            # Step 3: Calculate aggregations
            logger.info("\n[STEP 3] Calculating aggregations...")
            agg_df = self.calculate_aggregations(processed_df)
            
            # Step 4: Save processed data
            logger.info("\n[STEP 4] Saving processed data...")
            
            # Save daily data
            daily_success = self.save_processed_data(
                processed_df, 
                "daily_stocks"
            )
            
            # Save aggregations
            agg_success = self.save_processed_data(
                agg_df, 
                "stock_aggregations"
            )
            
            if daily_success and agg_success:
                logger.info("\n" + "=" * 60)
                logger.info("PROCESSING PIPELINE COMPLETED SUCCESSFULLY")
                logger.info("=" * 60)
                return True
            else:
                logger.error("Failed to save processed data")
                return False
                
        except Exception as e:
            logger.error(f"Processing pipeline failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
        
        finally:
            # Stop Spark session
            self.spark.stop()
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()

if __name__ == "__main__":
    processor = StockDataProcessor()
    success = processor.run_processing_pipeline()
    
    if success:
        print("\n✓ Spark processing completed successfully")
        exit(0)
    else:
        print("\n✗ Spark processing failed")
        exit(1)