from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, when, sum

spark = SparkSession.builder.appName("City Ride Analytics").master("local[*]").getOrCreate()


#T1: Create DataFrame from CSV
rides = spark.read.csv("miniproject_2_city_ride/rides.csv", header=True, inferSchema=True)
drivers = spark.read.csv("miniproject_2_city_ride/drivers.csv", header=True, inferSchema=True)

#T2: Display Schema & Preview
print("T2: Display Schema & Preview")
print("Printing Rides...")
rides.printSchema()
rides.show(5)
print("")
print("Printing Drivers...")
drivers.printSchema()
drivers.show(5)

#T3: Column Selection
print("T3: Column Selection")
selected = rides.select("ride_id", "pickup_location", "dropoff_location", "fare_amount")
selected.show(5)

# T4: Filtering Rides
print("T4: Filtering Rides")
filtered = rides.filter((col("distance_miles") > 5) & (col("ride_type") == "premium"))
filtered.show(5)

# T5: Adding a Derived Column — Fare Per Mile
print("T5: Adding a Derived Column — Fare Per Mile")
fare_per_mile = rides.withColumn("fare_per_mile", col("fare_amount")/col("distance_miles"))
fare_per_mile.show(5)

#T6: Removing Columns
print("T6: Removing Columns")
no_ride_type = rides.drop("ride_type")
no_ride_type.show(5)

#T7: Renaming Columns
print("T7: Renaming Columns")
renamed = rides.withColumnsRenamed({"pickup_location":"start_area", "dropoff_location":"end_area"})
renamed.show(5)

#T8: Aggregation — Total Revenue by Ride Type
print("T8: Aggregation — Total Revenue by Ride Type")
revenue = rides.groupBy("ride_type").sum("fare_amount")
revenue.show(5)

#T9: Aggregation — Average Rating per Driver
print("T9: Aggregation — Average Rating per Driver")
avg_rating = rides.groupBy("driver_id").avg("rating")
avg_rating.show(5)

#T10: Join — Enrich Rides with Driver Info
print("T10: Join — Enrich Rides with Driver Info")
inner = rides.join(drivers, "driver_id", "inner")
inner.show(5)

#T11: Set Operations — Combine Peak & Off-Peak Rides
print("T11: Set Operations — Combine Peak & Off-Peak Rides")
peak = rides.filter((col("ride_date") >= "2025-01-01") & (col("ride_date") < "2025-02-01"))
offpeak = rides.filter((col("ride_date") >= "2025-02-01") & (col("ride_date") < "2025-03-01"))
unioned = peak.union(offpeak)
unioned.show(5)

#T12: Spark SQL Queries
print("T12: Spark SQL Queries")
rides.createOrReplaceTempView("rides")
top_3 = spark.sql("SELECT * FROM rides ORDER BY fare_amount DESC LIMIT 3")
top_3.show()


#Additional Requirements

#O1: Multi-Column Sorting
print("O1: Multi-Column Sorting")
multi_sort = rides.orderBy("fare_amount", "distance_miles")
multi_sort.show(5)

#O2: Handling Nulls
print("O2: Handling Nulls")
print("Null Count: ",rides.filter(col('rating').isNull()).count())
null_filled = rides.fillna(0, subset=['rating'])
null_filled.show(5)

#O3: Conditional Column — Ride Category
print("O3: Conditional Column — Ride Category")
ride_cat = rides.withColumn("ride_category", when(col("distance_miles") < 3, "short").when(col("distance_miles") <= 8, "medium").otherwise("long"))
ride_cat.show(5)

#O4: Window Function — Running Revenue Total (Stretch Goal)
print("O4: Window Function — Running Revenue Total (Stretch Goal)")
windowSpec = Window.orderBy("ride_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
running_total = rides.withColumn("running_revenue_total", sum("fare_amount").over(windowSpec))
running_total.show(5)

#O5: Saving Results
inner.write.parquet("miniproject_2_city_ride/Ride_Analytics_Results")
revenue.write.csv("miniproject_2_city_ride/Ride_Analytics_Results2")
avg_rating.write.csv("miniproject_2_city_ride/Ride_Analytics_Results3")


spark.stop()