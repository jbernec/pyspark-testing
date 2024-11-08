import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.testing.utils import assertSchemaEqual

# Create a Spark session
spark = SparkSession.builder.appName("TestSession").getOrCreate()

# Create test data for the bronze layer
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), (None, 40), ("Eve", None)]
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
df = spark.createDataFrame(data, schema)

# Expected schema
expected_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
#@pytest.fixture
class TestSchema(object):
    pass
    """
        The test_schema function checks if the schema of the DataFrame matches the expected schema using
    """
    def test_schema(self):
        try:
            assertSchemaEqual(df.schema, expected_schema)
        except AssertionError as e:
            pytest.fail(f"Schema does not match the expected schema: {e}")

    """
        Test Age Column Values:
        - The `test_age_column_values` function checks if the values in the "age" column are valid.
        - It filters the DataFrame to find rows where the "age" column is not null and the age is either less than 0 or greater than 120.
        - It asserts that the count of such invalid ages is 0. If there are any invalid ages, the test will fail with an appropriate message.
    """
    def test_age_column_values(self):
        invalid_ages = df.filter((df.age.isNotNull()) & ((df.age < 0) | (df.age > 120))).count()
        assert invalid_ages == 0, f"Found invalid ages in the 'age' column"


    # Run the test
    if __name__ == "__main__":
        retcode = pytest.main(["-v", "-s"])
        assert retcode == 0, "The pytest invocation failed. See the log for details."