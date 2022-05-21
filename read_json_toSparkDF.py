import findspark
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import IntegerType, StringType
from pyspark import SparkConf
from pyspark.sql import SQLContext
session = SparkSession.builder.master('local[*]').appName('json_toSparkDF').getOrCreate()

path = '/data_files/mock_data.json'

json_schema = StructType([StructField('id', IntegerType()),
                          StructField('first_name', StringType()),
                          StructField('last_name', StringType()),
                          StructField('email', StringType()),
                          StructField('gender', StringType()),
                          StructField('ip_address', StringType())])

# Load json into Spark DataFrame
json_df = session.read \
        .format('json') \
        .option('path', path) \
        .schema(json_schema) \
        .load()

# Show the DataFrame
json_df.show(truncate=False)

