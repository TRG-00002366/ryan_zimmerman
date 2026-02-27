from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import SparkContext

# =============================================================================
# TASK 1: Understanding the Relationship
# =============================================================================

print("=== Task 1: SparkSession and SparkContext Relationship ===")


spark = SparkSession.builder \
    .appName("context app") \
    .master("local[*]") \
    .getOrCreate()
        
sc = spark.sparkContext  # Your code here (HINT: spark.sparkContext)


# Print app name from BOTH SparkSession and SparkContext
print(f"SparkSession app name: {spark.sparkContext.appName}")  # Complete
print(f"SparkContext app name: {sc.appName}")  # Complete

# Verify they share the same application ID
print(f"SparkSession app ID: {spark.sparkContext.applicationId}")  # Complete
print(f"SparkContext app ID: {sc.applicationId}")  # Complete


# Q1: Can you create a SparkContext after SparkSession exists?
# ANSWER: No
#
#

# Q2: What happens if you try? (You can test this if you want)
# ANSWER: Should result in an error since a context already exists in the session
#
#
# sc2 = SparkContext("local[*]", "test")
# Outputs this error
# ValueError: Cannot run multiple SparkContexts at once; existing SparkContext(app=context app, master=local[*]) created by getOrCreate at /home/ryan/ryan_zimmerman/week2/monday/excercise_context_comparison.py:14 


# =============================================================================
# TASK 2: RDD vs DataFrame Operations
# =============================================================================

print("\n=== Task 2: RDD vs DataFrame Operations ===")

rdd = sc.parallelize([1,2,3,4,5])  # Your code here (HINT: sc.parallelize(...))

# HINT: spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)], ["value"])
df = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)], ["value"])  # Your code here

rdd_doubled = rdd.map(lambda x: x*2)  # Your code here

df_doubled = df.withColumn("value", col("value") * 2)  # Your code here


# Print results
print("RDD doubled:")
print(rdd_doubled.collect())

print("DataFrame doubled:")
df_doubled.show()


rdd_to_df = rdd.map(lambda x: (x,)).toDF(["value"])  # Your code here

df_to_rdd = df.rdd  # Your code here (HINT: df.rdd)
print(df_to_rdd.collect())

# Q3: Which approach (RDD or DataFrame) felt more natural for this task?
# ANSWER: rdd
#
#

# Q4: What data type are the elements in df.rdd? (print first element to check)
# ANSWER: Row(value=x)
#
#


# =============================================================================
# TASK 3: Broadcast and Accumulator Access
# =============================================================================

print("\n=== Task 3: Broadcast and Accumulator ===")

# Example: {"NY": "New York", "CA": "California", "TX": "Texas"}
lookup_data = {"NY": "New York", "CA": "California", "TX": "Texas"}
broadcast_lookup = sc.broadcast(lookup_data)  # Your code here (HINT: sc.broadcast(...))


counter = sc.accumulator(0)  # Your code here (HINT: sc.accumulator(0))


# Create an RDD of state codes and:
# 1. Map each code to its full name using the broadcast variable
# 2. Count how many items are processed using the accumulator

states_rdd = sc.parallelize(["NY", "CA", "TX", "NY", "CA"])

# Your code here to use broadcast and accumulator
def map_func(x):
    counter.add(1)
    return (x, lookup_data[x])

result = states_rdd.map(map_func)

# Print results
print(f"Mapped states: {result.collect()}")
print(f"Items processed: {counter.value}")


# Q5: Why are broadcast and accumulator accessed via SparkContext instead of SparkSession?
# ANSWER: Spark context is included in session and handles operations on rdds. 
# Accumulators and broadcasts share information across an entire rdd which is split up between differnet machines.
#


# =============================================================================
# CONCEPTUAL QUESTIONS
# =============================================================================

print("\n=== Conceptual Questions ===")

# Answer these questions in the comments below:

# Q6: In a new PySpark 3.x project, which entry point would you use and why?
# ANSWER: Spark Session, it includes sparkcontext along with sql and hive context. It has more functionality, including the dataframe, so it is almost always used.
#
#
#

# Q7: You inherit legacy Spark 1.x code that uses SQLContext. 
#     What is the minimal change to modernize it?
# ANSWER: You could create a SparkSession at the start and then replace the sparkcontext creation with sc = spark.sparkContext
#
#
#

# Q8: Describe the relationship between SparkSession, SparkContext, 
#     SQLContext, and HiveContext (you can use ASCII art):
# ANSWER: Spark context is the main entry point and handles creation of rdds.
# SQLContext allows us to run sql queries and introduces the dataframe which is a high level abstraction of the rdd.
# HiveContext allows the use of HIVE SQL syntax.
# All 3 are encapsulated within SparkSession which can be used to configure all 3.
#
#


# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()