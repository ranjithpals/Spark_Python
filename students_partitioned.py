import findspark
findspark.init()

# Import Libraries
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F

# Set Log Level
# SparkContext.setLogLevel(logLevel='ERROR')

# Create Spark Session
spark = SparkSession.builder.appName("students").master("local[*]").getOrCreate()

# Read the input .csv file
df = spark.read \
    .format("csv") \
    .option("header", True) \
    .option("path", "C:/Users/Owner/Documents/Trendy_Tech/Week-18/Data/students_with_header.csv") \
    .load()

# Write the data as partitioned by subject and format parquet

df.write \
    .partitionBy("subject") \
    .parquet("C:/Users/Owner/Documents/Trendy_Tech/Week-18/parquet_data")

# Stop the Spark Session
spark.stop()



