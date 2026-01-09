CREATE EXTERNAL TABLE crypto_prices (
    currency STRING,
    price DOUBLE,
    `timestamp` STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION '/data/raw/prices';
