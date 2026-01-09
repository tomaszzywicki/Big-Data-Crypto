#!/bin/bash

hive -e 'MSCK REPAIR TABLE crypto_prices;'
