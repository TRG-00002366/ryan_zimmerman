"""
Exercise: Joins
===============
Week 2, Tuesday

Practice all join types with customer and order data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Joins").master("local[*]").getOrCreate()

# Customers
customers = spark.createDataFrame([
    (1, "Alice", "alice@email.com", "NY"),
    (2, "Bob", "bob@email.com", "CA"),
    (3, "Charlie", "charlie@email.com", "TX"),
    (4, "Diana", "diana@email.com", "FL"),
    (5, "Eve", "eve@email.com", "WA")
], ["customer_id", "name", "email", "state"])

# Orders
orders = spark.createDataFrame([
    (101, 1, "2023-01-15", 150.00),
    (102, 2, "2023-01-16", 200.00),
    (103, 1, "2023-01-17", 75.00),
    (104, 3, "2023-01-18", 300.00),
    (105, 6, "2023-01-19", 125.00),  # customer_id 6 does not exist!
    (106, 2, "2023-01-20", 180.00)
], ["order_id", "customer_id", "order_date", "amount"])

# Products (for multi-table join)
products = spark.createDataFrame([
    (101, "Laptop"),
    (102, "Phone"),
    (103, "Mouse"),
    (104, "Keyboard"),
    (107, "Monitor")  # Not in any order!
], ["order_id", "product_name"])

print("=== Exercise: Joins ===")
print("\nCustomers:")
customers.show()
print("Orders:")
orders.show()
print("Products:")
products.show()

# =============================================================================
# TASK 1: Inner Join (15 mins)
# =============================================================================

print("\n--- Task 1: Inner Join ---")

# 1a: Join customers and orders (only matching records)
# Show customer name, order_id, order_date, amount
customer_orders = customers.join(orders, customers.customer_id == orders.customer_id, "inner").select(customers.name, orders.order_id, orders.order_date, orders.amount)
customer_orders.show()

# 1b: How many orders have matching customers?
# HINT: Compare this count to total orders
print(f"Matching orders {customer_orders.count()}")
print(f"Total orders {orders.count()}")
print("")

# =============================================================================
# TASK 2: Left and Right Joins (20 mins)
# =============================================================================

print("\n--- Task 2: Left and Right Joins ---")

# 2a: LEFT JOIN - All customers, with order info where available
# Who has NOT placed any orders?
cust_ord_left = customers.join(orders, customers.customer_id == orders.customer_id, "left")
cust_ord_left.filter(cust_ord_left.order_id.isNull()).show()

# 2b: RIGHT JOIN - All orders, with customer info where available
# Which order has no matching customer?
cust_ord_right = customers.join(orders, customers.customer_id == orders.customer_id, "right")
cust_ord_right.filter(customers.customer_id.isNull()).show()

# 2c: What is the difference between the two results?
# Answer in a comment: Left join keeps all the rows in the left table with any matching rows in the right appended on
# Right join is the same but keeps all the rows in the right table and appends matching rows in the left table.
# 

# =============================================================================
# TASK 3: Full Outer Join (10 mins)
# =============================================================================

print("\n--- Task 3: Full Outer Join ---")

# 3a: Perform a FULL OUTER join between customers and orders
# All customers AND all orders should appear
cust_ord_full = customers.join(orders, customers.customer_id == orders.customer_id, "full")
cust_ord_full.show()

#  3b: Filter to show only rows where there is a mismatch
# (customer without order OR order without customer)
cust_ord_full.filter(customers.customer_id.isNull() | orders.customer_id.isNull()).show()

# =============================================================================
# TASK 4: Semi and Anti Joins (15 mins)
# =============================================================================

print("\n--- Task 4: Semi and Anti Joins ---")

# 4a: LEFT SEMI JOIN - Customers who HAVE placed orders
# Only customer columns should appear
cust_ord_semi = customers.join(orders, customers.customer_id == orders.customer_id, "left_semi")
cust_ord_semi.show()

# 4b: LEFT ANTI JOIN - Customers who have NOT placed orders
cust_ord_anti = customers.join(orders, customers.customer_id == orders.customer_id, "left_anti")
cust_ord_anti.show()

# 4c: When would you use anti join in real data work?
# Answer in a comment: When you want to filter for all rows that exisit in another table, but do not care about the columns in the other table
#
#

# =============================================================================
# TASK 5: Handling Duplicate Columns (15 mins)
# =============================================================================

print("\n--- Task 5: Handling Duplicate Columns ---")

# After joining customers and orders, both have customer_id

# 5a: Join and then DROP the duplicate customer_id column
customer_orders_dropped = customers.join(orders, "customer_id", "inner")
customer_orders_dropped.show()


# 5b: Alternative: Use aliases to reference specific columns
# HINT: customers.alias("c"), orders.alias("o")
c = customers.alias("c")
o = orders.alias("o")
alias_join = c.join(o, c.customer_id == o.customer_id).show()
# =============================================================================
# TASK 6: Multi-Table Join (15 mins)
# =============================================================================

print("\n--- Task 6: Multi-Table Join ---")

# 6a: Join customers -> orders -> products
# Show: customer name, order_id, amount, product_name
multi_join = customers.join(orders, customers.customer_id == orders.customer_id, "inner") \
.join(products, orders.order_id == products.order_id, "inner") \
.select(customers.name, orders.order_id, products.product_name)
multi_join.show()

# TODO 6b: What kind of join should you use when some orders might not have products?
# Depends on the business case, If you want to see all orders use left, if you want to see only orders with a product use inner
orders_products = orders.join(products, orders.order_id == products.order_id, "left")
orders_products.show()
# =============================================================================
# CHALLENGE: Real-World Scenarios (20 mins)
# =============================================================================

print("\n--- Challenge: Real-World Scenarios ---")

# 7a: Find the total spending per customer (only customers with orders)
# Use join + groupBy + sum
customer_orders_dropped.groupBy("customer_id").sum("amount").show()

# 7b: Find customers from CA who placed orders > $150
customer_orders_dropped.filter((col("state") == "CA") & (col("amount") > 150)).dropDuplicates(["customer_id"]).show()

# 7c: Find orders without valid product information
# (anti join pattern)
orders.join(products, orders.order_id == products.order_id, "left_anti").show()

# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()