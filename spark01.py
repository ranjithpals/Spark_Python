import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F

sc = SparkContext.getOrCreate()

# Create a Spark Session
spark = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()

#df = spark.createDataFrame(([1, 33],[2, 44], [1, 12], [1, 34]), ['dept', 'emp_id'])
#df = spark.createDataFrame((['', '']), ['dept', 'emp_id'])
# Create Empty RDD

emptyRDD = sc.emptyRDD()
#Create Schema
from pyspark.sql.types import StructType,StructField, StringType
schema = StructType([
  StructField('firstname', StringType(), True),
  StructField('middlename', StringType(), True),
  StructField('lastname', StringType(), True)
  ])

#Create empty DataFrame from empty RDD
df = spark.createDataFrame(emptyRDD,schema)
df.printSchema()

# Print Empty RDD
df.show()
collect = df.groupby('firstname').agg(F.collect_set(F.col('lastname')))
#print(collect.collect())