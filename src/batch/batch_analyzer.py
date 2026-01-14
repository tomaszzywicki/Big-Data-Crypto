from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main():
    spark = SparkSession.builder \
        .appName("CryptoBatchAnalyzer") \
        .config("spark.mongodb.output.uri", "mongodb://10.0.2.2:27017/crypto_database.batch_stats") \
        .enableHiveSupport() \
        .getOrCreate()

    # 1. Data Loading from Hive tables
    prices = spark.table("crypto_prices")
    sentiment = spark.table("sentiment_parquet")

    # 2. Data Preparation and Joining
    # Casting timestamp to TimestampType and creating a numeric version (seconds) for windowing
    prices = prices.withColumn("ts", F.to_timestamp("timestamp")) \
                   .withColumn("ts_long", F.col("ts").cast("long"))
    
    sentiment = sentiment.withColumn("s_ts", F.to_timestamp("timestamp"))

    joined = prices.join(sentiment, (prices.currency == sentiment.currency) & (prices.timestamp == sentiment.timestamp)) \
        .select(prices["*"], sentiment["long_short_ratio"])

    # 3. Window for rolling statistics (15 minutes = 900 seconds)
    window_spec = Window.partitionBy("currency").orderBy("ts_long").rangeBetween(-900, 0)

    # 4. Metric calculations
    analysis = joined \
        .withColumn("volatility", F.stddev("price").over(window_spec)) \
        .withColumn("prev_price", F.lag("price", 1).over(Window.partitionBy("currency").orderBy("ts"))) \
        .withColumn("momentum", (F.col("price") - F.col("prev_price")) / F.col("prev_price") * 100) \
        .withColumn("sma_4", F.avg("price").over(window_spec)) # Simple moving average

    # 5. Final aggregation (Hourly grouping for Dashboard)
    final_stats = analysis.groupBy("currency", "year", "month", "day", F.hour("ts").alias("hour")).agg(
        F.last("price").alias("last_price"),
        F.avg("price").alias("avg_price"),
        F.max("price").alias("max_price"),
        F.min("price").alias("min_price"),
        F.avg("long_short_ratio").alias("sentiment_strength"),
        F.avg("volatility").alias("rolling_volatility"),
        F.avg("momentum").alias("avg_momentum"),
        F.corr("long_short_ratio", "price").alias("sentiment_price_corr")
    )

    # 6. Writing to MongoDB
    final_stats.write.format("mongo").mode("overwrite").save()
    print(">>> BATCH ANALYSIS COMPLETED AND SAVED TO MONGODB")
    spark.stop()

if __name__ == "__main__":
    main()