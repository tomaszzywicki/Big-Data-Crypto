#!/bin/bash

# 1. Load the environment variables
source /home/vagrant/.bashrc

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HIVE_HOME=/usr/local/hive
export PATH=$PATH:$HADOOP_HOME/bin:$HIVE_HOME/bin

# Repair the Hive table to recognize new partitions for ssentiment data
/usr/local/hive/bin/hive -e "MSCK REPAIR TABLE sentiment_parquet;" >> /home/vagrant/PRO/Big-Data-Crypto/hive/sync.log 2>&1
# Repair the Hive table to recognize new partitions for crypto prices data
/usr/local/hive/bin/hive -e "MSCK REPAIR TABLE crypto_prices;" >> /home/vagrant/PRO/Big-Data-Crypto/hive/sync.log 2>&1