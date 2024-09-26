from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from src.constants import (
    RP1_MMHID_COL,
    DP4_DESCRIPTOR_COLS,
    DP4_TRIMMED_DESCRIPTOR_COLS,
    DP4_DESCRIPTOR_TRIMMED_CITY,
    RP1_TRIMMED_DESCRIPTOR_COLS,
    RP1_DESCRIPTOR_TRIMMED_NAME,
    VALID_FROM_COL,
    VALID_UNTIL_COL,
)


@pytest.fixture(scope="session")
def spark():
    """
    Creates a Spark session and stops it when tests finish running
    """
    spark_session = SparkSession.builder.master("local").appName("dp4-rp1-enrich").getOrCreate()
    yield spark_session
    spark_session.stop()


@pytest.fixture()
def test_split_mapped_txns_df_input_df(spark):
    return spark.createDataFrame(
        [
            (
                "Name_1",
                "State_1",
                "City_1",
                "Name_1",
                "State_1",
                "City_1",
                1,
                "Name_1",
                datetime(2020, 1, 1).date(),
                datetime(2021, 1, 1).date(),
                "X",
            ),
            (
                "Name_1 Trim ",
                " Trim State_1",
                " City_1   ",
                "Name_1 Trim",
                "Trim State_1",
                "City_1",
                1,
                "Name_1",
                datetime(2020, 1, 1).date(),
                datetime(2021, 1, 1).date(),
                " Trim X ",
            ),
            ("Name_2", "State_2", "City_2", "Name_2", "State_2", "City_2", None, None, None, None, "Y"),
        ],
        schema=[
            *DP4_DESCRIPTOR_COLS,
            *DP4_TRIMMED_DESCRIPTOR_COLS,
            RP1_MMHID_COL,
            RP1_DESCRIPTOR_TRIMMED_NAME,
            VALID_FROM_COL,
            VALID_UNTIL_COL,
            "scratch",
        ],
    )


@pytest.fixture()
def test_preprocess_city_input_txns_df(spark):
    return spark.createDataFrame(
        [
            ("Name_0", "State_0", "City0"),
            ("Name_1", "State_1", "City_1"),
            ("Name_2", "State_2", "City    2"),
            ("Name_3", "State_3", "City--3"),
        ],
        schema=DP4_TRIMMED_DESCRIPTOR_COLS,
    )


@pytest.fixture()
def test_preprocess_city_input_descriptors_df(spark):
    return spark.createDataFrame(
        [
            ("Name_00", "State_00", "C-i-t-y-00"),
            ("Name_11", "State_11", "C i..t//~ y11"),
        ],
        schema=RP1_TRIMMED_DESCRIPTOR_COLS,
    )


@pytest.fixture()
def test_postprocess_city_input_mapped_txns_df(spark):
    return spark.createDataFrame(
        [
            ("Name_0", "State_0", "City0", "City0", 1),
            ("Name_1", "State_1", "City1", "City_1", 2),
            ("Name_2", "State_2", "City2", "City    2", 3),
            ("Name_3", "State_3", "City3", "City--3", 4),
        ],
        schema=DP4_TRIMMED_DESCRIPTOR_COLS + ["_" + DP4_DESCRIPTOR_TRIMMED_CITY, RP1_MMHID_COL],
    )


@pytest.fixture()
def test_exact_join_input_txns_df(spark):
    return spark.createDataFrame(
        [
            ("Name_0", "State_0", "City0", "Name_0", "State_0", "City0"),
            ("Name_1 ", "State_1 ", "City_1 ", "Name_1", "State_1", "City_1"),
            ("Name_2", " State_2", " City    2", "Name_2", "State_2", "City    2"),
            ("Name_3 ", " State_3 ", " City--3 ", "Name_3", "State_3", "City--3"),
        ],
        schema=DP4_DESCRIPTOR_COLS + DP4_TRIMMED_DESCRIPTOR_COLS,
    )


@pytest.fixture()
def test_exact_join_input_descriptors_df(spark):
    return spark.createDataFrame(
        [
            ("Name_0", "State_0", "City0", 0, datetime(2000, 1, 1).date(), datetime(2100, 1, 1).date()),
            ("Name_1", "State_1", "City1", 1, datetime(2000, 1, 1).date(), datetime(2018, 5, 24).date()),
            ("Name_1", "State_1", "City1", 10, datetime(2018, 5, 25).date(), datetime(2100, 1, 1).date()),
            ("Name_2", "State_2", "City", 2, datetime(2000, 1, 1).date(), datetime(2100, 1, 1).date()),
            ("Name_4", "State_4", "City4", 4, datetime(2000, 1, 1).date(), datetime(2100, 1, 1).date()),
        ],
        schema=RP1_TRIMMED_DESCRIPTOR_COLS + [RP1_MMHID_COL, VALID_FROM_COL, VALID_UNTIL_COL],
    )


@pytest.fixture()
def test_startswith_join_input_txns_df(spark):
    return spark.createDataFrame(
        [
            ("Name_0", "State_0", "City0", "Name_0", "State_0", "City0"),
            ("1234567890_Name_0", "State_0", "City0", "1234567890_Name_0", "State_0", "City0"),
            ("1234567890_Name_1 ", "State_1 ", "City_1 ", "1234567890_Name_1", "State_1", "City_1"),
            ("1234567890_Name_2", " State_2", " City    2", "1234567890_Name_2", "State_2", "City    2"),
            ("1234567890_Name_3 ", " State_3 ", " City--3 ", "1234567890_Name_3", "State_3", "City--3"),
        ],
        schema=DP4_DESCRIPTOR_COLS + DP4_TRIMMED_DESCRIPTOR_COLS,
    )


@pytest.fixture()
def test_startswith_join_input_descriptors_df(spark):
    return spark.createDataFrame(
        [
            ("Name_0", "State_0", "City0", -1, datetime(2000, 1, 1).date(), datetime(2100, 1, 1).date()),
            ("1234567890_Name_0", "State_0", "City0", 0, datetime(2000, 1, 1).date(), datetime(2100, 1, 1).date()),
            ("1234567890_Name_00", "State_0", "City0", 100, datetime(2000, 1, 1).date(), datetime(2100, 1, 1).date()),
            ("1234567890_Name_000", "State_0", "City0", 1000, datetime(2000, 1, 1).date(), datetime(2100, 1, 1).date()),
            ("1234567890_Name_10", "State_1", "City1", 10, datetime(2000, 1, 1).date(), datetime(2018, 5, 24).date()),
            ("1234567890_Name_10", "State_1", "City1", 11, datetime(2018, 5, 25).date(), datetime(2100, 1, 1).date()),
            ("1234567890_Name_20", "State_2", "City", 20, datetime(2000, 1, 1).date(), datetime(2100, 1, 1).date()),
            ("1234567890_Name_4", "State_4", "City4", 4, datetime(2000, 1, 1).date(), datetime(2100, 1, 1).date()),
        ],
        schema=RP1_TRIMMED_DESCRIPTOR_COLS + [RP1_MMHID_COL, VALID_FROM_COL, VALID_UNTIL_COL],
    )
