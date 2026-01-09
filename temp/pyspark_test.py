from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# from logger import get_logger

# logger = get_logger(__name__)

SPARK_VERSION = "4.0.1"
SCALA_VERSION = "2.13"

KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"

SAVE_LOCATION = ""
PRICE_SCHEMA = StructType(
    [
        StructField("currency", StringType(), nullable=True),
        StructField("price", DoubleType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
    ]
)


def main():
    spark = (
        SparkSession.builder.appName("Test Kafka-PySpark integration")  # type: ignore
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # logger.info("Starting spark application")
    print("Starting spark application")

    kafkaStream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "crypto-prices")
        .option("startingOffsets", "earliest")
        .load()
    )

    # query = kafkaStream.writeStream.outputMode("append").format("console").start()

    # query2 = (
    #     kafkaStream.writeStream.format("json")
    #     .option("path", "../kafka_out")
    #     .option("checkpointLocation", "../chk_kafka")
    #     .start()
    # )

    df = kafkaStream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    query = df.writeStream.outputMode("append").format("console").start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
