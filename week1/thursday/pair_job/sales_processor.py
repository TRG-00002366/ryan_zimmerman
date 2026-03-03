#!/usr/bin/env python3
"""
Sales Data Processor
Processes sales data and generates summary reports.
"""

import argparse
from pyspark.sql import SparkSession

def parse_args():
    parser = argparse.ArgumentParser(description="Sales Processor")
    parser.add_argument("--input", required=True, help="Input data path")
    parser.add_argument("--output", required=True, help="Output path")
    parser.add_argument("--date", required=True, help="Processing date")
    return parser.parse_args()

def main():
    args = vars(parse_args())

    
    spark = SparkSession.builder \
        .appName("SalesProcessor") \
        .getOrCreate()
    
    # YOUR CODE: Implement data processing
    # 1. Read input data (create sample if needed)
    # 2. Filter by date
    # 3. Calculate totals by category
    # 4. Save results
    df = spark.read.csv(args["input"], header=True, inferSchema=True)
    df.filter(df["date"] > args["date"]).groupBy("catagory").sum("cost").write.csv(args["output"], header=True, mode="overwrite")

    spark.stop()

if __name__ == "__main__":
    main()