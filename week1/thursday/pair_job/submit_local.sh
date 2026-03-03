#!/bin/bash
# Local mode submission for testing

spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --py-files utils.py \
    sales_processor.py \
    --input data/sales.csv \
    --output output \
    --date 20240115