from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, last, when, expr, stddev
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Common schema for simplicity
schema = StructType([
    StructField("currency", StringType()),
    StructField("price", DoubleType()),
    StructField("long_short_ratio", DoubleType()),
    StructField("timestamp", StringType())
])

def write_to_mongo(batch_df, batch_id):
    if batch_df.limit(1).count() > 0:
        batch_df.write.format("mongo").mode("append").save()

def main():
    spark = SparkSession.builder \
        .appName("CryptoSpeedAnalyzer") \
        .config("spark.mongodb.output.uri", "mongodb://10.0.2.2:27017/crypto_database.live_alerts") \
        .getOrCreate()

    # Reading from both Kafka topics
    raw_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "crypto-prices,crypto-trends") \
        .load()

    parsed = raw_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*") \
        .withColumn("ts", col("timestamp").cast("timestamp")) \
        .withWatermark("ts", "1 minute")

    # Aggregation in 1-minute windows for anomaly detection
    live_analysis = parsed.groupBy(window("ts", "1 minute"), "currency").agg(
        last("price", ignorenulls=True).alias("live_price"),
        avg("long_short_ratio").alias("live_sentiment"),
        stddev("price").alias("live_vol")
    )

    live_analysis = live_analysis.withColumn("vol_ratio", col("live_vol") / col("live_price"))

    # Logic for detecting spikes and divergences
    alerts = live_analysis.withColumn("alert_type", 
        when(col("live_sentiment") > 2.5, "🔥 EXTREME LONG SENTIMENT")
        .when(col("live_sentiment") < 0.6, "🧊 EXTREME SHORT SENTIMENT")
        .when(col("vol_ratio") > 0.02, "⚡ HIGH VOLATILITY SPIKE")
        .otherwise("Normal")
    )

    query = alerts.writeStream \
        .foreachBatch(write_to_mongo) \
        .option("checkpointLocation", "/tmp/spark_checkpoints_speed") \
        .start()

    print(">>> SPEED LAYER IS RUNNING... MONITORING KAFKA")
    query.awaitTermination()

if __name__ == "__main__":
    main()