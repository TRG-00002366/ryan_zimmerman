from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
def main():
    # Step 1: Create SparkSession
    spark = SparkSession.builder \
        .appName("MyFirstJob") \
        .master("local[1]") \
        .getOrCreate()
    
    # Step 2: YOUR CODE HERE - Create some data
    sales_data = [
    ("Laptop", "Electronics", 999.99, 5),
    ("Mouse", "Electronics", 29.99, 50),
    ("Desk", "Furniture", 199.99, 10),
    ("Chair", "Furniture", 149.99, 20),
    ("Monitor", "Electronics", 299.99, 15),
    ]

    df = spark.createDataFrame(sales_data, ["product", "category", "price", "quantity"])

    # Step 3: YOUR CODE HERE - Perform transformations
    df.show()
    
    count = df.count()
   
    df = df.withColumn("revenue", df.price * df.quantity)
  
    elec = df.filter(df.category == "Electronics")
 
    catsum = df.groupBy("category").sum("revenue")

    # Step 4: YOUR CODE HERE - Show results
    print("Total products: " + str(count))
    print("")
    print("Revenue per product:")
    df.show()
    print("")
    print("Electronics only:")
    elec.show()
    print("")
    print("Revenue by category:")
    catsum.show()

    #Stretch Goals
    #1
    df.withColumn("revenue", col("price") * col("quantity")).agg(
        max("revenue")
    ).show()


    # Step 5: Clean up
    spark.stop()

if __name__ == "__main__":
    main()
