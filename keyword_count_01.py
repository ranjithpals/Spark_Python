
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F

# Create a Spark Context
#sc = SparkContext.getOrCreate

#spark.conf.set("spark.executor.memory", '8g')
#spark.conf.set('spark.executor.cores', '3')
#spark.conf.set('spark.cores.max', '3')
#spark.conf.set("spark.driver.memory",'8g')
#spark.conf.set("spark.dynamicAllocation.enabled","True")
#spark.conf.set("spark.shuffle.service.enabled", "True")

#conf = pyspark.SparkConf().setAll([('spark.executor.memory', '8g'), ('spark.executor.cores', '3'), ('spark.cores.max', '3'), ('spark.driver.memory','8g')])
conf = pyspark.SparkConf().setAll([('spark.dynamicAllocation.enabled', 'True'), ('spark.shuffle.service.enabled', 'True')])

sc = pyspark.SparkContext(conf=conf)

# Create a Spark Session
#spark = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()

# Read the text file
rdd1 = sc.textFile("bigLogNew.txt")

# Find the number of occurrences of alert type
rdd2 = rdd1.map(lambda x: (x.split(':')[0],1))
rdd3 = rdd2.reduceByKey(lambda x,y: x+y).collect()

for k, v in rdd3:
    print(k, v)

