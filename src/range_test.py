import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.testing.utils import assertSchemaEqual

@pytest.fixture(scope="session")
def spark_session():
    """Fixture for creating a SparkSession"""
    return SparkSession.builder.appName("TestSession").getOrCreate()

def test_dataframe_schema(spark_session):
    """Test that the dataframe has the expected schema"""

    expected_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), False)
    ])

    df = spark_session.createDataFrame(
        [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)],
        schema=expected_schema
    )

    assertSchemaEqual(df.schema, expected_schema)

def test_dataframe_not_null(spark_session):
    """Test that the dataframe has no null values in specified columns"""

    df = spark_session.createDataFrame(
        [(1, "Alice", 25), (2, None, 30), (3, "Charlie", 35)],
        ["id", "name", "age"]
    )

    assert df.filter(df.id.isNull()).count() == 0
    assert df.filter(df.age.isNull()).count() == 0

def test_dataframe_value_range(spark_session):
    """Test that the dataframe values are within the expected range"""

    df = spark_session.createDataFrame(
        [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)],
        ["id", "name", "age"]
    )

    assert df.filter(df.age < 18).count() == 0
    assert df.filter(df.age > 65).count() == 0

# Run the tests
if __name__ == "__main__":
    retcode = pytest.main(["-v", "-s"])
    assert retcode == 0, "The pytest invocation failed. See the log for details."