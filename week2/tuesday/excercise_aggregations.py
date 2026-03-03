"""
Exercise: Aggregations
======================
Week 2, Tuesday

Practice groupBy and aggregate functions on sales data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, min, max, countDistinct, any_value

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Aggregations").master("local[*]").getOrCreate()

# Sample sales data
sales = [
    ("2023-01", "Electronics", "Laptop", 1200, "Alice"),
    ("2023-01", "Electronics", "Phone", 800, "Bob"),
    ("2023-01", "Electronics", "Tablet", 500, "Alice"),
    ("2023-01", "Clothing", "Jacket", 150, "Charlie"),
    ("2023-01", "Clothing", "Shoes", 100, "Diana"),
    ("2023-02", "Electronics", "Laptop", 1300, "Eve"),
    ("2023-02", "Electronics", "Phone", 850, "Alice"),
    ("2023-02", "Clothing", "Jacket", 175, "Bob"),
    ("2023-02", "Clothing", "Pants", 80, "Charlie"),
    ("2023-03", "Electronics", "Laptop", 1100, "Frank"),
    ("2023-03", "Electronics", "Phone", 750, "Grace"),
    ("2023-03", "Clothing", "Shoes", 120, "Alice")
]

df = spark.createDataFrame(sales, ["month", "category", "product", "amount", "salesperson"])

print("=== Exercise: Aggregations ===")
print("\nSales Data:")
df.show()

# =============================================================================
# TASK 1: Simple Aggregations (15 mins)
# =============================================================================

print("\n--- Task 1: Simple Aggregations ---")

# 1a: Calculate total, average, min, and max amount across ALL sales
# Use agg() without groupBy
df.agg(
    spark_sum("amount"),
    avg("amount"),
    min("amount"),
    max("amount")
).show() 


# 1b: Count the total number of sales transactions
print(f"Number of sales: {df.count()}")
print("")

# 1c: Count distinct categories
df.agg(
    countDistinct("category")
).show()

# =============================================================================
# TASK 2: GroupBy with Single Aggregation (15 mins)
# =============================================================================

print("\n--- Task 2: GroupBy Single Aggregation ---")

#  2a: Total sales amount by category
df.groupBy("category").sum().show()

#  2b: Average sale amount by month
df.groupBy("month").avg().show()

#  2c: Count of transactions by salesperson
df.groupBy("salesperson").count().show()

# =============================================================================
# TASK 3: GroupBy with Multiple Aggregations (20 mins)
# =============================================================================

print("\n--- Task 3: GroupBy Multiple Aggregations ---")

# 3a: For each category, calculate:
# - Number of transactions
# - Total revenue
# - Average sale amount
# - Highest single sale
# Use meaningful aliases!
df.groupBy("category").agg(
    count("*").alias("Number of transactions"),
    spark_sum("amount").alias("Total revenue"),
    avg("amount").alias("Average sale amount"),
    max("amount").alias("Highest single sale")
).show()

# 3b: For each salesperson, calculate:
# - Number of sales
# - Total revenue
# - Distinct products sold (countDistinct)
df.groupBy("salesperson").agg(
    count("*").alias("Number of sales"),
    spark_sum("amount").alias("Total revenue"),
    countDistinct("product").alias("Distinct products sold")
).show()

# =============================================================================
# TASK 4: Multi-Column GroupBy (15 mins)
# =============================================================================

print("\n--- Task 4: Multi-Column GroupBy ---")

# 4a: Calculate total sales by month AND category
df.groupBy("month", "category").count().show()

# 4b: Find the top salesperson by month (hint: use multi-column groupBy)
monthly_sales = df.groupBy("month", "salesperson").agg(
    spark_sum("amount").alias("Total_revenue")
)
monthly_sales.groupBy("month").agg(
    any_value("salesperson").alias("salesperson"),
    max("Total_revenue").alias("Highest_This_Month")
).show()

# =============================================================================
# TASK 5: Filtering After Aggregation (15 mins)
# =============================================================================

print("\n--- Task 5: Filtering After Aggregation ---")

# 5a: Find categories with total revenue > 2000
df.groupBy("category").sum().filter(col("sum(amount)") > 2000).show()

# 5b: Find salespeople who made more than 2 transactions
df.groupBy("salesperson").agg(
    count("*").alias("Number of sales"),
).filter(col("Number of sales") > 2).show()


# 5c: Find month-category combinations with average sale > 500
monthly_category = df.groupBy("month", "category").agg(
    avg("amount").alias("average sales")
).filter(col("average sales") > 500).show()

# =============================================================================
# CHALLENGE: Business Questions (20 mins)
# =============================================================================

print("\n--- Challenge: Business Questions ---")

# 6a: Which category had the highest average transaction value?
highest_avg_cat = df.groupBy("category").avg().agg(
    max("avg(amount)")
)
highest_avg_cat.show()

# 6b: Who is the top salesperson by total revenue?
monthly_sales = df.groupBy("month", "salesperson").sum().agg(
    max("sum(amount)")
)
monthly_sales.show()

# 6c: Which month had the most diverse products sold?
# HINT: Use countDistinct on product column
df.groupBy("month").agg(
    countDistinct("product").alias("monthly_unique_sales")
).agg(
    max("monthly_unique_sales")
).show()

# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()