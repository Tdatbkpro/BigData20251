from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import LinearRegression
import logging
from datetime import datetime
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockAnalyzer:
    def __init__(self):
        self.spark = self._create_spark_session()
        self.hdfs_base_path = "hdfs://namenode:9000/stock_data"
    
    def _create_spark_session(self):
        """Create Spark session for analysis"""
        return SparkSession.builder \
            .appName("StockAnalyzer") \
            .master("spark://spark-master:7077") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
    
    def load_processed_data(self):
        """Load processed data from HDFS"""
        try:
            path = f"{self.hdfs_base_path}/processed/daily_stocks"
            
            logger.info(f"Loading processed data from: {path}")
            
            # Try Parquet first, then CSV
            try:
                df = self.spark.read.parquet(path)
            except:
                df = self.spark.read \
                    .option("header", "true") \
                    .csv(f"{path}_csv")
            
            if df.count() == 0:
                logger.warning("No processed data found")
                return None
            
            # Convert numeric columns
            numeric_cols = ["open", "high", "low", "close", "volume", "daily_return"]
            for col_name in numeric_cols:
                if col_name in df.columns:
                    df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
            
            logger.info(f"Loaded {df.count()} records")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load data: {str(e)}")
            return None
    
    def calculate_technical_indicators(self, df):
        """Calculate technical indicators"""
        if df is None:
            return None
        
        try:
            # Create window for each symbol
            window_spec = Window.partitionBy("symbol").orderBy("date")
            
            # Moving averages
            df = df.withColumn("ma_5", avg("close").over(window_spec.rowsBetween(-4, 0)))
            df = df.withColumn("ma_10", avg("close").over(window_spec.rowsBetween(-9, 0)))
            df = df.withColumn("ma_20", avg("close").over(window_spec.rowsBetween(-19, 0)))
            df = df.withColumn("ma_50", avg("close").over(window_spec.rowsBetween(-49, 0)))
            
            # RSI calculation (simplified)
            df = df.withColumn("price_change", col("close") - lag("close", 1).over(window_spec))
            df = df.withColumn("gain", when(col("price_change") > 0, col("price_change")).otherwise(0))
            df = df.withColumn("loss", when(col("price_change") < 0, -col("price_change")).otherwise(0))
            
            # Average gain and loss over 14 periods
            avg_gain = avg("gain").over(window_spec.rowsBetween(-13, 0))
            avg_loss = avg("loss").over(window_spec.rowsBetween(-13, 0))
            
            df = df.withColumn("rs", when(col("avg_loss") == 0, 100).otherwise(col("avg_gain") / col("avg_loss")))
            df = df.withColumn("rsi", 100 - (100 / (1 + col("rs"))))
            
            # Bollinger Bands
            df = df.withColumn("std_20", stddev("close").over(window_spec.rowsBetween(-19, 0)))
            df = df.withColumn("bb_upper", col("ma_20") + (2 * col("std_20")))
            df = df.withColumn("bb_lower", col("ma_20") - (2 * col("std_20")))
            df = df.withColumn("bb_position", (col("close") - col("bb_lower")) / (col("bb_upper") - col("bb_lower")))
            
            # MACD (simplified)
            df = df.withColumn("ema_12", avg("close").over(window_spec.rowsBetween(-11, 0)))
            df = df.withColumn("ema_26", avg("close").over(window_spec.rowsBetween(-25, 0)))
            df = df.withColumn("macd", col("ema_12") - col("ema_26"))
            
            # Volume indicators
            df = df.withColumn("volume_ma_10", avg("volume").over(window_spec.rowsBetween(-9, 0)))
            df = df.withColumn("volume_ratio", col("volume") / col("volume_ma_10"))
            
            logger.info("Technical indicators calculated")
            return df
            
        except Exception as e:
            logger.error(f"Error calculating technical indicators: {str(e)}")
            return df
    
    def detect_anomalies(self, df):
        """Detect anomalous trading patterns"""
        if df is None:
            return None
        
        try:
            window_spec = Window.partitionBy("symbol")
            
            # Volume anomalies (z-score > 3)
            df = df.withColumn("volume_mean", avg("volume").over(window_spec))
            df = df.withColumn("volume_std", stddev("volume").over(window_spec))
            df = df.withColumn("volume_zscore", 
                              (col("volume") - col("volume_mean")) / col("volume_std"))
            df = df.withColumn("is_volume_anomaly", 
                              abs(col("volume_zscore")) > 3)
            
            # Price anomalies
            price_window = Window.partitionBy("symbol").orderBy("date")
            df = df.withColumn("price_change_pct", 
                              ((col("close") - lag("close", 1).over(price_window)) / 
                               lag("close", 1).over(price_window) * 100))
            
            df = df.withColumn("price_change_abs", abs(col("price_change_pct")))
            df = df.withColumn("is_price_anomaly", col("price_change_abs") > 10)
            
            # Gap anomalies (opening gap)
            df = df.withColumn("prev_close", lag("close", 1).over(price_window))
            df = df.withColumn("opening_gap_pct", 
                              ((col("open") - col("prev_close")) / col("prev_close") * 100))
            df = df.withColumn("is_gap_anomaly", abs(col("opening_gap_pct")) > 5)
            
            # Combined anomaly score
            df = df.withColumn("anomaly_score",
                              when(col("is_volume_anomaly"), 1).otherwise(0) +
                              when(col("is_price_anomaly"), 1).otherwise(0) +
                              when(col("is_gap_anomaly"), 1).otherwise(0))
            
            df = df.withColumn("has_anomaly", col("anomaly_score") >= 2)
            
            logger.info(f"Anomaly detection completed")
            return df
            
        except Exception as e:
            logger.error(f"Error detecting anomalies: {str(e)}")
            return df
    
    def cluster_stocks(self, df, n_clusters=5):
        """Cluster stocks based on features"""
        try:
            # Prepare feature matrix
            feature_cols = ["avg_daily_return", "price_volatility", "avg_volume"]
            
            # Filter and prepare data
            feature_df = df.select(
                "symbol", 
                "avg_daily_return", 
                "price_volatility", 
                "avg_volume"
            ).dropna()
            
            if feature_df.count() == 0:
                logger.warning("No data for clustering")
                return None
            
            # Assemble features
            assembler = VectorAssembler(
                inputCols=feature_cols,
                outputCol="features",
                handleInvalid="skip"
            )
            
            feature_vector = assembler.transform(feature_df)
            
            # Scale features
            scaler = StandardScaler(
                inputCol="features",
                outputCol="scaled_features",
                withStd=True,
                withMean=True
            )
            
            scaler_model = scaler.fit(feature_vector)
            scaled_data = scaler_model.transform(feature_vector)
            
            # Apply KMeans clustering
            kmeans = KMeans(
                k=n_clusters,
                seed=42,
                featuresCol="scaled_features",
                predictionCol="cluster"
            )
            
            model = kmeans.fit(scaled_data)
            clustered_df = model.transform(scaled_data)
            
            # Get cluster centers
            centers = model.clusterCenters()
            logger.info(f"Cluster centers: {centers}")
            
            # Add cluster descriptions
            cluster_descriptions = {
                0: "Low Risk, Low Return",
                1: "High Risk, High Return",
                2: "Stable, Moderate Return",
                3: "Volatile, Speculative",
                4: "Blue Chip, Stable"
            }
            
            clustered_df = clustered_df.withColumn(
                "cluster_description",
                when(col("cluster") == 0, cluster_descriptions[0])
                .when(col("cluster") == 1, cluster_descriptions[1])
                .when(col("cluster") == 2, cluster_descriptions[2])
                .when(col("cluster") == 3, cluster_descriptions[3])
                .otherwise(cluster_descriptions[4])
            )
            
            logger.info(f"Clustering completed. Cluster distribution:")
            clustered_df.groupBy("cluster", "cluster_description").count().show()
            
            return clustered_df
            
        except Exception as e:
            logger.error(f"Error in clustering: {str(e)}")
            return None
    
    def generate_trading_signals(self, df):
        """Generate trading signals based on technical indicators"""
        if df is None:
            return None
        
        try:
            # Simple moving average crossover strategy
            df = df.withColumn("ma_crossover",
                              when((col("ma_5") > col("ma_20")) & 
                                   (lag("ma_5", 1).over(Window.partitionBy("symbol").orderBy("date")) <= 
                                    lag("ma_20", 1).over(Window.partitionBy("symbol").orderBy("date"))),
                                   "BUY")
                              .when((col("ma_5") < col("ma_20")) & 
                                    (lag("ma_5", 1).over(Window.partitionBy("symbol").orderBy("date")) >= 
                                     lag("ma_20", 1).over(Window.partitionBy("symbol").orderBy("date"))),
                                    "SELL")
                              .otherwise("HOLD"))
            
            # RSI signals
            df = df.withColumn("rsi_signal",
                              when(col("rsi") < 30, "OVERSOLD")
                              .when(col("rsi") > 70, "OVERBOUGHT")
                              .otherwise("NEUTRAL"))
            
            # Bollinger Band signals
            df = df.withColumn("bb_signal",
                              when(col("close") < col("bb_lower"), "OVERSOLD")
                              .when(col("close") > col("bb_upper"), "OVERBOUGHT")
                              .otherwise("WITHIN_BANDS"))
            
            # Volume spike signal
            df = df.withColumn("volume_signal",
                              when(col("volume_ratio") > 2, "HIGH_VOLUME")
                              .when(col("volume_ratio") < 0.5, "LOW_VOLUME")
                              .otherwise("NORMAL_VOLUME"))
            
            # Combined signal
            df = df.withColumn("combined_signal",
                              when((col("ma_crossover") == "BUY") & 
                                   (col("rsi_signal") == "OVERSOLD") &
                                   (col("bb_signal") == "OVERSOLD"),
                                   "STRONG_BUY")
                              .when((col("ma_crossover") == "SELL") & 
                                    (col("rsi_signal") == "OVERBOUGHT") &
                                    (col("bb_signal") == "OVERBOUGHT"),
                                    "STRONG_SELL")
                              .when(col("ma_crossover") == "BUY", "BUY")
                              .when(col("ma_crossover") == "SELL", "SELL")
                              .otherwise("HOLD"))
            
            logger.info("Trading signals generated")
            return df
            
        except Exception as e:
            logger.error(f"Error generating trading signals: {str(e)}")
            return df
    
    def save_analysis_results(self, df, analysis_type):
        """Save analysis results to HDFS"""
        if df is None:
            return False
        
        try:
            output_path = f"{self.hdfs_base_path}/analytics/{analysis_type}"
            
            df.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(output_path)
            
            logger.info(f"Analysis results saved to: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save analysis results: {str(e)}")
            return False
    
    def generate_insights_report(self, df):
        """Generate insights report from analyzed data"""
        if df is None:
            return {}
        
        try:
            insights = {}
            
            # Top gainers today
            latest_date = df.agg(max("date")).collect()[0][0]
            today_df = df.filter(col("date") == latest_date)
            
            if today_df.count() > 0:
                top_gainers = today_df.filter(col("daily_return") > 0) \
                    .orderBy(col("daily_return").desc()) \
                    .select("symbol", "exchange", "daily_return", "close", "volume") \
                    .limit(10) \
                    .collect()
                
                insights["top_gainers"] = [
                    {
                        "symbol": row.symbol,
                        "exchange": row.exchange,
                        "daily_return": float(row.daily_return),
                        "price": float(row.close),
                        "volume": int(row.volume)
                    }
                    for row in top_gainers
                ]
            
            # Top volume stocks
            top_volume = today_df.orderBy(col("volume").desc()) \
                .select("symbol", "exchange", "volume", "close") \
                .limit(10) \
                .collect()
            
            insights["top_volume"] = [
                {
                    "symbol": row.symbol,
                    "exchange": row.exchange,
                    "volume": int(row.volume),
                    "price": float(row.close)
                }
                for row in top_volume
            ]
            
            # Stocks with anomalies
            anomaly_stocks = df.filter(col("has_anomaly") == True) \
                .select("symbol", "exchange", "date", "anomaly_score") \
                .distinct() \
                .limit(20) \
                .collect()
            
            insights["anomaly_stocks"] = [
                {
                    "symbol": row.symbol,
                    "exchange": row.exchange,
                    "date": row.date.strftime("%Y-%m-%d"),
                    "anomaly_score": int(row.anomaly_score)
                }
                for row in anomaly_stocks
            ]
            
            # Trading signals summary
            if "combined_signal" in df.columns:
                signal_counts = df.groupBy("combined_signal").count().collect()
                insights["trading_signals"] = {
                    row.combined_signal: int(row.count)
                    for row in signal_counts
                }
            
            # Save insights to HDFS
            insights_path = f"{self.hdfs_base_path}/analytics/insights"
            insights_df = self.spark.createDataFrame([{"insights": json.dumps(insights)}])
            insights_df.write.mode("overwrite").json(insights_path)
            
            logger.info("Insights report generated")
            return insights
            
        except Exception as e:
            logger.error(f"Error generating insights report: {str(e)}")
            return {}
    
    def run_analysis_pipeline(self):
        """Run complete analysis pipeline"""
        logger.info("=" * 60)
        logger.info("STARTING STOCK ANALYSIS PIPELINE")
        logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        try:
            # Step 1: Load data
            logger.info("\n[STEP 1] Loading processed data...")
            df = self.load_processed_data()
            
            if df is None or df.count() == 0:
                logger.error("No data to analyze")
                return False
            
            # Step 2: Calculate technical indicators
            logger.info("\n[STEP 2] Calculating technical indicators...")
            df = self.calculate_technical_indicators(df)
            
            # Step 3: Detect anomalies
            logger.info("\n[STEP 3] Detecting anomalies...")
            df = self.detect_anomalies(df)
            
            # Step 4: Generate trading signals
            logger.info("\n[STEP 4] Generating trading signals...")
            df = self.generate_trading_signals(df)
            
            # Step 5: Save analysis results
            logger.info("\n[STEP 5] Saving analysis results...")
            self.save_analysis_results(df, "technical_analysis")
            
            # Step 6: Generate insights
            logger.info("\n[STEP 6] Generating insights report...")
            insights = self.generate_insights_report(df)
            
            # Print insights summary
            if insights:
                logger.info("\n" + "=" * 60)
                logger.info("ANALYSIS INSIGHTS SUMMARY")
                logger.info("=" * 60)
                
                if "top_gainers" in insights:
                    logger.info("\nTop Gainers (Today):")
                    for stock in insights["top_gainers"][:3]:
                        logger.info(f"  {stock['symbol']}: {stock['daily_return']:.2f}%")
                
                if "trading_signals" in insights:
                    logger.info("\nTrading Signals Distribution:")
                    for signal, count in insights["trading_signals"].items():
                        logger.info(f"  {signal}: {count}")
            
            logger.info("\n" + "=" * 60)
            logger.info("ANALYSIS PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            logger.error(f"Analysis pipeline failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
        
        finally:
            self.spark.stop()

if __name__ == "__main__":
    analyzer = StockAnalyzer()
    success = analyzer.run_analysis_pipeline()
    
    if success:
        print("\n✓ Stock analysis completed successfully")
        exit(0)
    else:
        print("\n✗ Stock analysis failed")
        exit(1)