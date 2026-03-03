"""
Exercise: Set Operations
========================
Week 2, Wednesday

Practice union, intersect, except operations on customer data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Set Ops").master("local[*]").getOrCreate()

# January customers
jan_customers = spark.createDataFrame([
    (1, "Alice", "alice@email.com"),
    (2, "Bob", "bob@email.com"),
    (3, "Charlie", "charlie@email.com"),
    (4, "Diana", "diana@email.com")
], ["customer_id", "name", "email"])

# February customers  
feb_customers = spark.createDataFrame([
    (2, "Bob", "bob@email.com"),
    (4, "Diana", "diana@email.com"),
    (5, "Eve", "eve@email.com"),
    (6, "Frank", "frank@email.com")
], ["customer_id", "name", "email"])

# March customers (different column order!)
mar_customers = spark.createDataFrame([
    ("grace@email.com", "Grace", 7),
    ("henry@email.com", "Henry", 8),
    ("bob@email.com", "Bob", 2)  # Returning customer
], ["email", "name", "customer_id"])

print("=== Exercise: Set Operations ===")
print("\nJanuary Customers:")
jan_customers.show()
print("February Customers:")
feb_customers.show()
print("March Customers (different column order!):")
mar_customers.show()

# =============================================================================
# TASK 1: Union Operations (20 mins)
# =============================================================================

print("\n--- Task 1: Union ---")

# 1a: Union January and February customers (keep duplicates)
jan_feb = jan_customers.union(feb_customers)
jan_feb.show()

# 1b: Union January and February, then remove duplicates
distinct = jan_feb.distinct()
distinct.show()

# 1c: Try union with March customers - what happens?
# Use unionByName to fix it

#Union will error since the columns are in different orders
feb_march = feb_customers.unionByName(mar_customers)
feb_march.show()

# 1d: How many unique customers do you have across all three months?
unique_customers = jan_customers.union(feb_customers).unionByName(mar_customers).distinct()
print(f"Unique Customers: {unique_customers.count()}")

# =============================================================================
# TASK 2: Intersect (15 mins)
# =============================================================================

print("\n--- Task 2: Intersect ---")

# 2a: Find customers who appear in BOTH January AND February
jan_and_feb = jan_customers.intersect(feb_customers)
jan_and_feb.show()

# 2b: Verify the result makes sense - who are the returning customers?
#Only Bob and Diana are in both january and febuary and that is the only 2 rows we see

# =============================================================================
# TASK 3: Subtract/Except (15 mins)
# =============================================================================

print("\n--- Task 3: Subtract/Except ---")

# 3a: Find customers in January who did NOT return in February
jan_except_feb = jan_customers.exceptAll(feb_customers)
jan_except_feb.show()

# 3b: Find NEW customers in February (not in January)
feb_except_jan = feb_customers.exceptAll(jan_customers)
feb_except_jan.show()

# 3c: Business question: What is the customer churn from Jan to Feb?
# Answer in a comment: Gained 2 customers and lost 2, so churn is 0%


# =============================================================================
# TASK 4: Distinct and DropDuplicates (15 mins)
# =============================================================================

print("\n--- Task 4: Deduplication ---")

# Combined data with duplicates
all_data = jan_customers.union(feb_customers)

# 4a: Use distinct() to remove exact duplicate rows
distinct = all_data.distinct()
distinct.show()

# 4b: Use dropDuplicates() on email column only
# (Keep first occurrence of each email)
email_distinct = all_data.dropDuplicates(["email"])
email_distinct.show()

# 4c: What is the difference between distinct() and dropDuplicates()?
# Answer in a comment: dropDuplicates can specify columns while distinct considers all


# =============================================================================
# CHALLENGE: Data Reconciliation (20 mins)
# =============================================================================

print("\n--- Challenge: Data Reconciliation ---")

# System A data (source)
source = spark.createDataFrame([
    (1, "Product A", 100),
    (2, "Product B", 200),
    (3, "Product C", 300)
], ["id", "name", "price"])

# System B data (target)
target = spark.createDataFrame([
    (1, "Product A", 100),
    (2, "Product B", 250),  # Price difference!
    (4, "Product D", 400)   # New product!
], ["id", "name", "price"])

# 5a: Find exact matches between source and target
matches = source.intersect(target)

#  5b: Find records in source but not in target (or different)
soruce_only = source.exceptAll(target)

#  5c: Find records in target but not in source (or different)
target_only = target.exceptAll(source)

# TODO 5d: Create a reconciliation report showing:
# - Matched count
# - Source-only count
# - Target-only count
print(f"Match Count: {matches.count()}" )
print(f"Source Only Count: {soruce_only.count()}" )
print(f"Target Only Count: {target_only.count()}" )

# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()