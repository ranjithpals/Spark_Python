# Import libraries
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import IntegerType, StringType, TimestampType, BooleanType
from pyspark.sql.functions import col, lag, lead, count, when, sum, round
from pyspark.sql.window import Window

# Build a Spark Session
spark = SparkSession.builder.master("local[*]").appName("imo_solution").getOrCreate()


def read_file(session=spark, file='imo_new.csv'):
    # Defining the Schema for the data
    search_data = StructType([StructField("user_id", IntegerType()),
                              StructField("timestamp", TimestampType()),
                              StructField("search", StringType())])

    # Read the data from sample file
    df = session.read \
        .format('csv') \
        .option('path', file) \
        .option('inferSchema', False) \
        .option('header', False) \
        .schema(search_data) \
        .load()

    # Return the input data as DataFrame
    return df


def source_count(input_data):
    source_rec_count = input_data.count()
    print('Input file records: ', source_rec_count)
    return source_rec_count


def transform_data(input_data):
    # Define the window function - group by user_id and order by timestamp needed to compare subsequent searches
    window_func = Window.partitionBy("user_id") \
                      .orderBy(col("timestamp"))
    # Find if the subsequent search entries based on the window function are similar
    df1 = input_data.withColumn("auto_w_next", col("search").startswith(lead(col("search"), 1).over(window_func)))
    df2 = df1.withColumn("auto_w_prev", lead(col("search"), 1).over(window_func).startswith(col("search")))
    # df2.show

    # Identify the auto-suggests and different searches based on the search results.
    # Auto-search entries are marked TRUE
    df3 = df2.withColumn("auto_gen", when(col("auto_w_next") | col("auto_w_prev"), True).otherwise(False))

    # Return the transformed dataframe
    return df3


def search_counts(df):
    # Total Non-auto-searches
    non_auto = df.filter(col("auto_gen") != True).count()
    print(f'Total Non-auto-searches: {non_auto}')

    # Total auto-searches
    auto = df.filter(col("auto_gen") == True).count()
    print(f'Total auto-searches: {auto}')

    # Total searches
    searches = df.count()
    print(f'Total searches: {searches}')

    return non_auto, auto, searches


def find_user_search_count(df):
    # Count of total search for each user
    user_count = df.groupBy("user_id").agg(count("auto_gen").alias("searches"))
    return user_count


def find_agg_users(df):
    # Count of auto and unique searches per user.
    agg = df.groupBy("user_id").agg(sum(col("auto_gen").cast("long")) \
                .alias("auto_search"), count(col("auto_gen")) \
                .alias("total_searches"))
    return agg


def write_to_disk(df):
    # Write the agg_per_user to disk
    agg_per_user.write \
        .format('csv') \
        .option('path', '/output/') \
        .mode('overwrite') \
        .save()


if __name__ == "__main__":
    # Read the input data file
    source_data = read_file(file='imo_new.csv')

    # View the Sample data
    # source_data.show(50, False)
    # Transform the data
    output_df = transform_data(source_data)
    # Display the result of each of the entry
    output_df.select('user_id', 'timestamp', 'search', 'auto_gen').show()

    # Count of searches per user
    agg_user = find_user_search_count(output_df)
    # agg_user.show()

    agg_per_user = find_agg_users(output_df)
    # Percentage of unique searches
    agg_per_user.withColumn("unique_search%", round((col("total_searches") - col("auto_search")) * 100 \
                                                    / col("total_searches"), 1)).show()
    # print(agg_per_user.first())