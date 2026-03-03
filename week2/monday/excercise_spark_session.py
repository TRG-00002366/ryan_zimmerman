"""
Exercise: SparkSession Setup and Configuration
===============================================
Week 2, Monday

Complete the TODOs below to practice creating and configuring SparkSession objects.
"""

from pyspark.sql import SparkSession

# =============================================================================
# TASK 1: Basic SparkSession Creation
# =============================================================================


#   - App name: "MyFirstSparkSQLApp"
#   - Master: "local[*]"
# HINT: Use SparkSession.builder.appName(...).master(...).getOrCreate()

spark = SparkSession.builder.appName("MyFirstSparkSQLApp").master("local[*]").getOrCreate()  

#   - Spark version
#   - Application ID
#   - Default parallelism
# HINT: Access these via spark.version, spark.sparkContext.applicationId, etc.

print("=== Task 1: Basic SparkSession ===")
# Your print statements here
print(f"Spark Version: ", spark.sparkContext.version)
print(f"Application ID: ", spark.sparkContext.applicationId)
print(f"Parallelism: ", spark.sparkContext.defaultParallelism)

# to verify your session works
data = [(1, "Ryan", 23), (2, "Juan", 34), (3, "Seth", 24), (4, "Navdeep", 21), (5, "Ronald", 23)]  
columns = ["id", "name", "age"] 
df = spark.createDataFrame(data, columns)  # Create DataFrame

# Show the DataFrame
df.show()


# =============================================================================
# TASK 2: Configuration Exploration
# =============================================================================

print("\n=== Task 2: Configuration ===")

# HINT: Use spark.conf.get("spark.sql.shuffle.partitions")

print(f"Shuffle partitions: {spark.conf.get("spark.sql.shuffle.partitions")}")  # Complete this


# Some options: spark.driver.memory, spark.executor.memory, spark.sql.adaptive.enabled
print(f"{spark.conf.get("spark.app.name")} : {spark.conf.get("spark.executor.id")} : {spark.conf.get("spark.master")}")  # Complete this


#2c: Try changing spark.sql.shuffle.partitions at runtime
# Does it work? Add a comment explaining what happens.


# Your code here
spark.conf.set("spark.sql.shuffle.partitions", 150)
print(f"Shuffle partitions: {spark.conf.get("spark.sql.shuffle.partitions")}")  # It works, any new shuffles will have 150 instead of 200, more data goes in each partition since their is less of them,

# =============================================================================
# TASK 3: getOrCreate() Behavior
# =============================================================================

print("\n=== Task 3: getOrCreate() Behavior ===")

# 3a: Create another reference using getOrCreate with a DIFFERENT app name
spark2 = SparkSession.builder.appName("DifferentName").getOrCreate() 


#  3b: Check which app name is actually used
print(f"spark app name: {spark.sparkContext.appName}")
print(f"spark2 app name: {spark2.sparkContext.appName}")


#  3c: Are spark and spark2 the same object? Check with 'is' operator
if spark is spark2:
    print("they are the same")
else:
    print("they are different")

#  3d: EXPLAIN IN A COMMENT: Why does getOrCreate() behave this way?
# Your explanation:
# Spark Session will not make a new object if it can copy one
# Since 2 spark contexts cannot be made at the same time they need to share it
#


# =============================================================================
# TASK 4: Session Cleanup
# =============================================================================

print("\n=== Task 4: Cleanup ===")

#  4a: Stop the SparkSession properly
print(spark.sparkContext._jsc.sc().isStopped())
spark.stop()


# 4b: Verify the session has stopped
# HINT: Check spark.sparkContext._jsc.sc().isStopped() before stopping
#print(spark.sparkContext._jsc.sc().isStopped())
#Spark is now null, running this causes an error

# =============================================================================
# STRETCH GOALS (Optional)
# =============================================================================
print("\n=== STRETCH GOALS ===")
# Stretch 1: Create a helper function that builds a SparkSession with your
# preferred default configurations

def create_my_spark_session(app_name, shuffle_partitions=100):
    """
    Creates a SparkSession with custom defaults.
    
    Args:
        app_name: Name of the Spark application
        shuffle_partitions: Number of shuffle partitions (default: 100)
    
    Returns:
        SparkSession object
    """
    return SparkSession.builder.appName(app_name).master("local[*]").config("spark.sql.shuffle.partitions", shuffle_partitions).getOrCreate()


# Stretch 2: Enable Hive support
# HINT: Use .enableHiveSupport() in the builder chain
# Note: This may fail if Hive is not configured - that's okay!
def create_hive_spark_session(app_name, shuffle_partitions=100):
    return SparkSession.builder.appName(app_name).master("local[*]").config("spark.sql.shuffle.partitions", shuffle_partitions) \
        .enableHiveSupport().getOrCreate()

# Stretch 3: List all configuration options
# HINT: spark.sparkContext.getConf().getAll() returns all settings
print(spark.sparkContext.getConf().getAll())