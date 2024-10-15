# for i in range(1, 12):
#     a = str(i).zfill(2)
#     print(a)
#     print(type(a))

# from pyspark.sql import SparkSession
# from pyspark.sql.types import * 


# Create Spark session
# spark = SparkSession.builder.master("local").appName("example").getOrCreate()

# # Sample data
# data = [("Alice", 29), ("Bob", 31)]
# schema = StructType([StructField("name", StringType(), True), StructField("age", IntegerType(), True)])

# # Create DataFrame
# df = spark.createDataFrame(data, schema=schema)
# df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])

# df = spark.createDataFrame([('Alice', 1)], "name: string, age: int")
# df.createOrReplaceTempView("people")

# df.show()


# Create a temporary view
# df.createOrReplaceTempView("people")

# Querying the temporary view using SQL
# spark.sql("SELECT * FROM people").show()

# spark.createDataFrame([], "name: string, age: int").show()


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Define schema using StructType
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Use parallelize to create an empty RDD
empty_rdd = spark.sparkContext.parallelize([])

# Create DataFrame using the empty RDD and schema
empty_df = spark.createDataFrame(empty_rdd, schema)

# Show the DataFrame
empty_df.show()
