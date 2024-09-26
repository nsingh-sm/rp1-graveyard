import pytest
from pyspark.sql import SparkSession
from datetime import datetime


@pytest.fixture(scope="session")
def spark():
    """
    Creates a Spark session and stops it when tests finish running
    """
    spark_session = SparkSession.builder.master("local").appName("rp1-standardization").getOrCreate()
    yield spark_session
    spark_session.stop()


@pytest.fixture()
def compress_table_input_df(spark):
    """
    input_df:
    +-----+-----+----------+----------+
    |col_1|col_2|     start|       end|
    +-----+-----+----------+----------+
    |    a|    b|2019-12-01|2020-02-29|
    |    a|    b|2020-03-01|2020-05-31|
    |    a|    b|2020-09-01|2020-11-30|
    |    a|  BBB|2021-03-01|2021-05-31|
    |    a|    b|2021-06-01|2021-08-31|
    |    x|    y|2019-12-01|2020-02-29|
    |    x|    Y|2020-03-01|2020-05-31|
    |    m|    n|2019-12-01|2020-02-29|
    +-----+-----+----------+----------+
    """
    return spark.createDataFrame(
        [
            ["a", "b", datetime(2019, 12, 1).date(), datetime(2020, 2, 29).date()],  # Mar
            ["a", "b", datetime(2020, 3, 1).date(), datetime(2020, 5, 31).date()],  # Jun - same mmhid
            ["a", "b", datetime(2020, 9, 1).date(), datetime(2020, 11, 30).date()],  # Dec - same mmhid
            ["a", "BBB", datetime(2021, 3, 1).date(), datetime(2021, 5, 31).date()],  # Jun - changed mmhid
            ["a", "b", datetime(2021, 6, 1).date(), datetime(2021, 8, 31).date()],  # Jun - change in mmhid
            ["x", "y", datetime(2019, 12, 1).date(), datetime(2020, 2, 29).date()],  # Mar
            ["x", "Y", datetime(2020, 3, 1).date(), datetime(2020, 5, 31).date()],  # Jun - changed mmhid
            ["m", "n", datetime(2019, 12, 1).date(), datetime(2020, 2, 29).date()],  # Mar
        ],
        schema=["col_1", "col_2", "start", "end"],
    )
