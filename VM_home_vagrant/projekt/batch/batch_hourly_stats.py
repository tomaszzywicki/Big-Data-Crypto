import sys
import os
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, mean, stddev
from pyspark.sql.window import Window
from pyspark.sql import functions as F


LOG_DIR = "/home/vagrant/projekt/logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

MONGO_SPARK_CONNECTOR_VERSION = "2.12:3.0.1"
MONGO_DATABASE = "crypto_database"
MONGO_COLLECTION = "hourly_stats"


def main():
    dt = datetime.now() - timedelta(hours=1)
    target_year, target_month, target_day, target_hour = dt.year, dt.month, dt.day, dt.hour

    # Jeśli podano argumenty, nadpisujemy
    if len(sys.argv) == 5:
        target_year = int(sys.argv[1])
        target_month = int(sys.argv[2])
        target_day = int(sys.argv[3])
        target_hour = int(sys.argv[4])

    log_filename = (
        f"{LOG_DIR}/batch_hourly_stats_{target_year}{target_month:02d}{target_day:02d}_{target_hour:02d}.log"
    )

    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        filename=log_filename,
        filemode="a",
        format="%(asctime)s %(levelname)s %(message)s",
        level=logging.INFO,
    )
    logger = logging.getLogger()

    logger.info(f"Starting job for: {target_year}-{target_month:02d}-{target_day:02d} {target_hour:02d}:00")

    logger.info("Initializing Spark...")
    spark = (
        SparkSession.builder.appName("Batch Hourly Stats")
        .config(
            "spark.jars.packages", f"org.mongodb.spark:mongo-spark-connector_{MONGO_SPARK_CONNECTOR_VERSION}"
        )
        .config("spark.mongodb.output.uri", f"mongodb://127.0.0.1:27017/{MONGO_DATABASE}.{MONGO_COLLECTION}")
        .getOrCreate()
    )
    logger.info("Spark has been initialized succesfully")

    try:
        PRICES_PATH = f"/data/raw/prices/"
        # Read prices from current hour, previous day
        prices_df = spark.read.parquet(PRICES_PATH).filter(
            (col("year") == target_year)
            & (col("month") == target_month)
            & (col("day") == target_day)
            & (F.hour(col("timestamp")) == target_hour)
        )
        logger.info(f"Data read. Count: {prices_df.count()}")

        if prices_df.count() == 0:
            logger.warning("No data found for given period. Exiting.")
            return

        window = Window.partitionBy("currency", "year", "month", "day", "hour").orderBy("timestamp")

        enriched_df = (
            prices_df.withColumn("hour", F.hour(col("timestamp")))
            .withColumn("open_price", F.first("price").over(window))
            .withColumn("close_price", F.last("price").over(window))
        )

        final_batch_view = enriched_df.groupBy("currency", "year", "month", "day", "hour").agg(
            F.first("open_price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("close_price").alias("close"),
            F.count("price").alias("read_count"),
            mean(col("price")).alias("mean_price"),
            stddev(col("price")).alias("std_price"),
        )

        final_batch_view = final_batch_view.withColumn(
            "return_pct", (col("close") - col("open")) / col("open") * 100
        ).withColumn("spread_pct", (col("high") - col("low")) / col("low") * 100)

        # save batch view to MongoDB
        final_batch_view.write.format("mongo").mode("append").save()
        logger.info("Batch view successfully saved to MongoDB.")

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
