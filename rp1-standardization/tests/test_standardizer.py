from src.standardizer import (
    _add_prefix,
    _filter_data_where_all_columns_are_null,
    _filter_data_for_latest_delivery_date,
)
from src.utils import impute_state_and_city_columns, clean_agg_merch_columns
import pyspark.sql.functions as F
from datetime import datetime


def test_add_prefix():
    cols = ["test_1", "test_2"]
    assert _add_prefix(cols) == ["rp1_test_1", "rp1_test_2"]


def test_filter_data_where_all_columns_are_null(spark):
    input_df = spark.createDataFrame(
        [
            (1, "A", "Good-1"),
            (2, None, "Good-2"),
            (None, None, "Bad-1"),
            (None, "C", "Good-3"),
            (None, None, "Bad-2"),
        ],
        schema=["col_1", "col_2", "col_3"],
    )

    resultant_df = (
        _filter_data_where_all_columns_are_null(input_df, ["col_1", "col_2"]).orderBy(F.col("col_3")).toPandas()
    )

    assert (resultant_df["col_3"] == ["Good-1", "Good-2", "Good-3"]).all()


def test_filter_data_for_latest_delivery_date(spark):
    input_df = spark.createDataFrame(
        [
            (1, "A", datetime(2020, 1, 1).date(), datetime(2020, 3, 20).date(), datetime(2020, 3, 20).date()),
            (1, "B", datetime(2020, 1, 1).date(), datetime(2020, 3, 20).date(), datetime(2023, 9, 18).date()),
            (2, "C", datetime(2020, 3, 15).date(), datetime(2020, 6, 5).date(), datetime(2020, 6, 5).date()),
            (2, "D", datetime(2020, 3, 15).date(), datetime(2020, 6, 5).date(), datetime(2020, 12, 30).date()),
            (2, "E", datetime(2020, 3, 15).date(), datetime(2020, 6, 5).date(), datetime(2021, 12, 30).date()),
            (3, "F", datetime(2020, 11, 27).date(), datetime(2021, 1, 12).date(), datetime(2021, 1, 12).date()),
        ],
        schema=["col_1", "col_2", "start_date", "end_date", "delivery_date"],
    )

    resultant_df = (
        _filter_data_for_latest_delivery_date(input_df, "start_date", "end_date")
        .orderBy(F.col("delivery_date"))
        .toPandas()
    )

    print(resultant_df)

    assert (resultant_df["col_1"] == [3, 2, 1]).all()
    assert (resultant_df["col_2"] == ["F", "E", "B"]).all()
    assert (
        resultant_df["start_date"]
        == [datetime(2020, 11, 27).date(), datetime(2020, 3, 15).date(), datetime(2020, 1, 1).date()]
    ).all()
    assert (
        resultant_df["end_date"]
        == [datetime(2021, 1, 12).date(), datetime(2020, 6, 5).date(), datetime(2020, 3, 20).date()]
    ).all()
    assert (
        resultant_df["delivery_date"]
        == [datetime(2021, 1, 12).date(), datetime(2021, 12, 30).date(), datetime(2023, 9, 18).date()]
    ).all()


def test_impute_state_and_city_columns(spark):
    input_df = spark.createDataFrame(
        [
            ("t'challa", "AA", "Wakanda"),
            ("Batman", None, "Gotham      XX"),
            ("Tyrion", None, "Castle Rock USA"),
            ("Jamie", None, "Castle Rock US"),
            ("Vivek", "HK", "+1 (332) 248-7042"),
        ],
        schema=["rp1_original_name", "rp1_original_state", "rp1_original_city"],
    )

    resultant_df = impute_state_and_city_columns(input_df).orderBy(F.col("imputed_state")).fillna("NULL").toPandas()

    expected_result_df = (
        spark.createDataFrame(
            [
                ("Tyrion", None, "Castle Rock USA", None, "Castle Rock USA"),
                ("t'challa", "AA", "Wakanda", "AA", "Wakanda"),
                ("Vivek", "HK", "+1 (332) 248-7042", "HK", "+1 (332) 248-7042"),
                ("Jamie", None, "Castle Rock US", "US", "Castle Rock"),
                ("Batman", None, "Gotham      XX", "XX", "Gotham"),
            ],
            schema=["rp1_original_name", "rp1_original_state", "rp1_original_city", "imputed_state", "imputed_city"],
        )
        .fillna("NULL")
        .toPandas()
    )

    assert (resultant_df["rp1_original_name"] == expected_result_df["rp1_original_name"]).all()
    assert (resultant_df["rp1_original_state"] == expected_result_df["rp1_original_state"]).all()
    assert (resultant_df["rp1_original_city"] == expected_result_df["rp1_original_city"]).all()
    assert (resultant_df["imputed_state"] == expected_result_df["imputed_state"]).all()
    assert (resultant_df["imputed_city"] == expected_result_df["imputed_city"]).all()


def test_clean_agg_merch_columns(spark):
    # fmt off:
    test_input_txns_df = spark.createDataFrame(
        [
            ("Coke", "5600", "Cola", 30000, "12", "Coca Cola (NSR)", "Coca Cola (NSR)"),
            ("Coke Soft Drink", "5600", "Cola", 50000, "13", "Coca Cola", "Coca Cola"),
            ("C0ke", "5600", None, 500, "14", "Not Coca Cola (ON-LINE)", "Not Coca Cola"),
            ("Fantaa", "5329", None, 1500, "15", "Fanta (ON-LINE)||Fanta (NSR)||Fanta", "Fanta||Fanta (NSR)"),
            ("Fake Pepsi, actually C0ke", "5599", "Pepsi", 700, None, None, None),
        ],
        schema=["dp4_name", "dp4_mcc", "bsm_brand", "n_txns", "rp1_mmhid", "rp1_amn", "rp1_pamn"],
    )
    # fmt: on

    cleaned_df = test_input_txns_df.withColumn("rp1_amn", clean_agg_merch_columns("rp1_amn")).orderBy(
        F.col("n_txns").desc()
    )

    assert cleaned_df.toPandas()["rp1_amn"].values.tolist() == [
        "Coca Cola",
        "Coca Cola",
        "Fanta",
        None,
        "Not Coca Cola",
    ]

    cleaned_df = test_input_txns_df.withColumn("rp1_pamn", clean_agg_merch_columns("rp1_pamn")).orderBy(
        F.col("n_txns").asc()
    )

    assert cleaned_df.toPandas()["rp1_pamn"].values.tolist() == [
        "Not Coca Cola",
        None,
        "Fanta",
        "Coca Cola",
        "Coca Cola",
    ]
