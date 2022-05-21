# Import libraries
import findspark
import imo_solution as imo_sol
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, lead, count, when, sum, round

# CONSTANTS - TO BE MOVED TO CONFIG FILE
FILE_NAME = 'imo_new.csv'
NUM_RECORDS = 35
TEST_USER_ID = 4245
TEST_USER_ID_SEARCH = 'coronavirus symptom'
TEST_USER_ID_SEARCH_TIMESTAMP = '2021-01-06 07:55:34'
SEARCH_COUNT_TEST_USER = 10
AUTO_SEARCH_COUNT_TEST_USER = 6

# Build a Spark Session
spark = SparkSession.builder.master("local[*]").appName("imo_solution").getOrCreate()

# Call the function to read source file
df = imo_sol.read_file(spark, FILE_NAME)
transform_df = imo_sol.transform_data(df)


# validate the number of source file records
def test_source_rec_count():
    # Validate the number of records
    assert df.count() == NUM_RECORDS


# Validate the first record (first occurrence of the day) of a given
def test_source_data_format(user_id=TEST_USER_ID):
    # Validate the source data format
    row = df.filter(df.user_id == user_id).orderBy(df.timestamp.asc()).first()
    i_user_id = row.user_id
    i_timestamp = str(row.timestamp)
    i_search = row.search
    assert i_user_id == TEST_USER_ID
    assert i_timestamp == TEST_USER_ID_SEARCH_TIMESTAMP
    assert i_search == TEST_USER_ID_SEARCH


# Validate the count of searches of a given user
def test_user_search_count(user_id=TEST_USER_ID):
    # Validate the count of searches
    temp_df = imo_sol.find_user_search_count(transform_df)
    row = temp_df.filter(temp_df.user_id == user_id).first()
    assert row.searches == SEARCH_COUNT_TEST_USER, "Validation of search count for given user was NOT successful"


# Validate the count of searches of a given user
def test_user_auto_search_count(user_id=TEST_USER_ID):
    # Validate the count of searches
    temp_df = imo_sol.find_agg_users(transform_df)
    row = temp_df.filter(temp_df.user_id == user_id).first()
    assert row.auto_search == AUTO_SEARCH_COUNT_TEST_USER


if __name__ == "__main__":
    try:
        test_source_rec_count()
        print("PASS: ", "Source Data file records match the Spark DataFrame count")
    except Exception as msg:
        print(msg)
        print("FAIL: ", "Source Data file records do not match the Spark DataFrame count")

    try:
        test_source_data_format()
        print("PASS: ", "Validation of source data fields for given user record was successful")
    except Exception as msg:
        print(msg)
        print("FAIL: ", "Validation of source data fields for given user record was NOT successful")

    try:
        test_user_search_count()
        print("PASS: ", "Validation of search count for given user was successful")
    except Exception as msg:
        print(msg)
        print("FAIL: ", "Validation of search count for given user was NOT successful")

    try:
        test_user_auto_search_count()
        print("PASS: ", "Validation of auto search count for given user was successful")
    except Exception as msg:
        print(msg)
        print("FAIL: ", "Validation of auto search count for given user was NOT successful")
