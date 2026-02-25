from pyspark import SparkContext

sc = SparkContext("local[*]", "RDDBasics")

# 1. Create RDD from a Python list
numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
print(f"Numbers: {numbers.collect()}")
print(f"Partitions: {numbers.getNumPartitions()}")
print("")

# 2. Create RDD with explicit partitions
# YOUR CODE: Create the same list with exactly 4 partitions
explicit_partition_rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 4)
print(f"Numbers: {explicit_partition_rdd.collect()}")
print(f"Partitions: {explicit_partition_rdd.getNumPartitions()}")
print("")

# 3. Create RDD from a range
# YOUR CODE: Create RDD from range(1, 101)
range_rdd = sc.parallelize(range(1,101))
print(f"Numbers: {range_rdd.collect()}")
print(f"Partitions: {range_rdd.getNumPartitions()}")
print("")

#task 2
# Given: numbers RDD [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# Task A: Square each number
# Expected: [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
squared = numbers.map(lambda x: x**2)
print(squared.collect())
print("")

# Task B: Convert to strings with prefix
# Expected: ["num_1", "num_2", "num_3", ...]
prefixed = numbers.map(lambda x: "num_"+str(x))
print(prefixed.collect())
print("")


#task 3
# Task A: Keep only even numbers
# Expected: [2, 4, 6, 8, 10]
evens = numbers.filter(lambda x: x%2==0)
print(evens.collect())
print("")
# Task B: Keep numbers greater than 5
# Expected: [6, 7, 8, 9, 10]
greater_than_5 = numbers.filter(lambda x: x > 5)
print(greater_than_5.collect())
print("")
# Task C: Combine - even AND greater than 5
# Expected: [6, 8, 10]
combined = numbers.filter(lambda x: x%2==0 and x > 5)
print(combined.collect())
print("")


#task 4
# Given sentences
sentences = sc.parallelize([
    "Hello World",
    "Apache Spark is Fast",
    "PySpark is Python plus Spark"
])

# Task A: Split into words (use flatMap)
# Expected: ["Hello", "World", "Apache", "Spark", ...]
words = sentences.flatMap(lambda x: x.split(' '))
print(words.collect())
print("")
# Task B: Create pairs of (word, length)
# Expected: [("Hello", 5), ("World", 5), ...]
word_lengths = sentences.flatMap(lambda x: x.split(' ')).map(lambda x: (x, len(x)))
print(word_lengths.collect())
print("")


#task5
# Given: log entries
logs = sc.parallelize([
    "INFO: User logged in",
    "ERROR: Connection failed",
    "INFO: Data processed",
    "ERROR: Timeout occurred",
    "DEBUG: Cache hit"
])

# Pipeline: Extract only ERROR messages, convert to uppercase words
# 1. Filter to keep only ERROR lines
# 2. Split each line into words
# 3. Convert each word to uppercase
# Expected: ["ERROR:", "CONNECTION", "FAILED", "ERROR:", "TIMEOUT", "OCCURRED"]
error_words = logs.filter(lambda line: "ERROR" in line).flatMap(lambda x: x.split(' ')).map(lambda x: x.upper())
print(error_words.collect())

sc.stop()