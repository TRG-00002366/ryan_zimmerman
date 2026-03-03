"""
Exercise: Datasets and Type Safety
==================================
Week 2, Wednesday

Explore the Dataset/DataFrame paradigm and type-aware patterns in PySpark.
"""

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Datasets").master("local[*]").getOrCreate()

print("=== Exercise: Datasets and Type Safety ===")

# =============================================================================
# TASK 1: Understanding Row Objects (15 mins)
# =============================================================================

print("\n--- Task 1: Row Objects ---")

#  1a: Create 3 Row objects for employees
# Each should have: name (string), age (int), department (string)
employee1 = Row(name="Ryan", age=22, department="Developer")
employee2 = Row(name="Jaun", age=32, department="Developer")
employee3 = Row(name="Carla", age=26, department="HR")


#  1b: Create a DataFrame from these Row objects
df = spark.createDataFrame([employee1,employee2,employee3])
df.show()

# 1c: Access data from the first row using:
# - Attribute access (row.name)
# - Index access (row[0])
# - Key access (row["name"])
attribute = employee1.name
index = employee2[0]
key = employee3["name"]
print(f"By attribute: {attribute} - By Index: {index} - Bykey: {key}")
print("")

# 1d: Convert a Row to a dictionary
print(employee1.asDict())

# =============================================================================
# TASK 2: Explicit Schemas (20 mins)
# =============================================================================

print("\n--- Task 2: Explicit Schemas ---")

# TODO 2a: Define a schema for a Product type with:
# - id (IntegerType, not nullable)
# - name (StringType, nullable)
# - price (DoubleType, nullable)
# - category (StringType, nullable)

product_schema = StructType([
    StructField(name="id", dataType=IntegerType(), nullable=False),
    StructField(name="name", dataType=StringType(), nullable=True),
    StructField(name="price", dataType=DoubleType(), nullable=True),
    StructField(name="category", dataType=StringType(), nullable=True)
])


# 2b: Create sample product data and create DataFrame with the schema
products_data = [
    (1, "Laptop", 999.99, "Electronics"),
    (2, "Coffee Maker", 49.99, "Home"),
    (3, "Running Shoes", None, "Sports") 
]

df = spark.createDataFrame(products_data, product_schema)

# 2c: Verify the schema matches your definition
print(df.schema)

# 2d: What happens if you try to create data that violates the schema?
# Try creating data with wrong types and observe the error
# (Comment out after testing to avoid breaking the script)

bad_data = [
    (1, 1, 999.99, "Electronics"),
    (2, 2, "hello", "Home"),
    (3, 3, None, "Sports") 
]
#spark.createDataFrame(bad_data, product_schema).show()
#Get the error field price: DoubleType() can not accept object 'hello' in type <class 'str'>.
#It seems spark can convert numbers to strings, but will error if you try to pass a string into a numeric column

# =============================================================================
# TASK 3: Schema Validation (20 mins)
# =============================================================================

print("\n--- Task 3: Schema Validation ---")

# Incoming data that needs validation
incoming_data = spark.createDataFrame([
    (1, "Alice", 30, "Engineering"),
    (2, None, 25, "Marketing"),      # Null name - invalid?
    (3, "Charlie", -5, "Sales"),     # Negative age - invalid!
    (4, "Diana", 150, "Engineering") # Age too high - invalid!
], ["id", "name", "age", "department"])

print("Incoming data:")
incoming_data.show()

# 3a: Create a validation function that checks:
# - name is not null
# - age is between 18 and 100
# Return two DataFrames: valid and invalid
def validate_employees(df):
    """
    Validates employee data.
    Returns: (valid_df, invalid_df)
    """
    # Your code here
    valid = df.filter((col("name").isNotNull()) & (col("age").between(lit(18), lit(100))))
    invalid = df.exceptAll(valid)
    return valid, invalid


# 3b: Apply validation and show results
valid_df, invalid_df = validate_employees(incoming_data)
print("Valid:")
valid_df.show()
print("Invalid:")
invalid_df.show()


# =============================================================================
# TASK 4: Working with RDD for Typed Transformations (20 mins)
# =============================================================================

print("\n--- Task 4: RDD Typed Transformations ---")

# Sample DataFrame
people = spark.createDataFrame([
    ("Alice", 30),
    ("Bob", 25),
    ("Charlie", 35)
], ["name", "age"])

# 4a: Convert DataFrame to RDD
rdd = people.rdd

# 4b: Use map() to transform each row, adding a category:
# - "Young" if age < 30
# - "Senior" if age >= 30
# Create new Row objects in the map function
def add_category(row):
    row_dict = row.asDict()
    row_dict["category"] = "Senior"
    if row_dict["age"] < 30:
        row_dict["category"] = "Young"
    return Row(**row_dict)

rdd = rdd.map(add_category)

# 4c: Convert the transformed RDD back to DataFrame
people = rdd.toDF()
people.show()

# 4d: Why might you use RDD transformations instead of DataFrame?
# Answer in a comment: RDD's are lower level, with a map function you can implement a fine-grained mapping with a custom function that might be difficult with a df.


# =============================================================================
# TASK 5: Schema Evolution (15 mins)
# =============================================================================

print("\n--- Task 5: Schema Evolution ---")

# Version 1 data
v1_data = spark.createDataFrame([
    (1, "Widget A", 19.99),
    (2, "Widget B", 29.99)
], ["id", "name", "price"])

# Version 2 data (has additional column)
v2_data = spark.createDataFrame([
    (3, "Widget C", 39.99, "Electronics"),
    (4, "Widget D", 49.99, "Home")
], ["id", "name", "price", "category"])

print("V1 schema:")
v1_data.printSchema()
print("V2 schema:")
v2_data.printSchema()

# 5a: Evolve V1 data to match V2 schema
# Add the missing "category" column with null values
v1_data = v1_data.withColumn("category", lit(None).cast(StringType()))

# 5b: Combine V1 and V2 data using unionByName
v1_data.unionByName(v2_data).show()

# =============================================================================
# CONCEPTUAL QUESTIONS
# =============================================================================

print("\n--- Conceptual Questions ---")

# Answer in comments:

# Q1: In Scala/Java, what is the difference between Dataset[Row] and Dataset[Person]?
# ANSWER: These are datasets but they store a differnet types of data. One stores Row objects and the other stores Person objects.
#

# Q2: Why does Python not have true typed Datasets like Scala?
# ANSWER: Because unlike Java, Python is dynamically typed. Java/Scala are statically typed so types can be enforced at compile time.
#

# Q3: What are the benefits of using explicit schemas in production?
# ANSWER: Clearly indicate to future devolpers what types should be used. Manual checks can also be made to ensure correct types.
#

# Q4: When would schema validation at the application level be important?
# ANSWER: To ensure a user is supplying the correct datatypes.
#


# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()