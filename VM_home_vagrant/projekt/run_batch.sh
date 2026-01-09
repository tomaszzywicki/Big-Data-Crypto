#!/bin/bash

export JAVA_HOME=/usr/local/java
export SPARK_HOME=/usr/local/spark
export PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH

PYTHON_SCRIPT="/home/vagrant/projekt/batch/batch_hourly_stats.py"

LOG_FILE="/home/vagrant/projekt/logs/cron_shell.log"

echo "[$(date)] Starting Spark Batch Job..." >> $LOG_FILE

$SPARK_HOME/bin/spark-submit \
    --master "local[*]" \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
    --conf "spark.ui.enabled=false" \
    $PYTHON_SCRIPT >> $LOG_FILE 2>&1

echo "[$(date)] Job finished with exit code $?" >> $LOG_FILE
