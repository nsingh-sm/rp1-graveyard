import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Creates a Spark session and stops it when tests finish running
    """
    spark_session = SparkSession.builder.master("local").appName("rp1-tagging-rec-precompute").getOrCreate()
    yield spark_session
    spark_session.stop()
