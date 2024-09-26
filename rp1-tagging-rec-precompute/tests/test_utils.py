from src.utils import (
    build_output_field_to_n_txns_df,
    enrich_transitive_relationships_df,
    filter_same_company_collisions,
)

from src.constants import TRANSITIVITY_OUTPUT_FIELD, OUTPUT_FIELD_AFTER_TRANSITIVITY


def test_build_output_field_to_n_txns_df(spark):
    # fmt off:
    test_input_txns_df = spark.createDataFrame(
        [
            (1, "Coke", "5600", "Cola", 30000, "12", "Coca Cola (NSR)"),
            (2, "Coke Soft Drink", "5600", "Cola", 50000, "13", "Coca Cola"),
            (3, "C0ke", "5600", None, 500, "14", "Not Coca Cola (ON-LINE)"),
            (4, "Fantaa", "5329", None, 1500, "15", "Fanta (ON-LINE)||Fanta (NSR)||Fanta"),
            (5, "Fake Pepsi, actually C0ke", "5599", "Pepsi", 700, None, None),
        ],
        schema=[
            "dp4_transaction_id",
            "dp4_name",
            "dp4_mcc",
            TRANSITIVITY_OUTPUT_FIELD,
            "n_txns",
            "rp1_mmhid",
            "rp1_amn",
        ],
    )
    # fmt: on

    result_df = build_output_field_to_n_txns_df(test_input_txns_df).toPandas()

    assert len(result_df) == 2
    assert sorted(result_df[OUTPUT_FIELD_AFTER_TRANSITIVITY].values.tolist()) == ["Cola", "Pepsi"]
    assert result_df[result_df[OUTPUT_FIELD_AFTER_TRANSITIVITY] == "Cola"][
        f"{OUTPUT_FIELD_AFTER_TRANSITIVITY}_txns"
    ].to_list() == [80000]
    assert result_df[result_df[OUTPUT_FIELD_AFTER_TRANSITIVITY] == "Pepsi"][
        f"{OUTPUT_FIELD_AFTER_TRANSITIVITY}_txns"
    ].to_list() == [700]


def test_filter_same_company_collisions(spark):
    # fmt off:
    test_transitive_relationships_df = spark.createDataFrame(
        [
            ("Walmart 123", 10484, "WalmartBrand", "WalmartOptics"),
            ("Southwest Sauce", 420, "SouthwestAirlinesInFlight", "Veeba"),
            ("SW* Midair Sandwich", 1795, "SouthwestAirlinesBrand", "SouthwestAirlinesInFlight"),
        ],
        schema=["dp4_name", "n_txns", TRANSITIVITY_OUTPUT_FIELD, OUTPUT_FIELD_AFTER_TRANSITIVITY],
    )
    # fmt: on

    # fmt off:
    brand_id_to_company_ids_map = spark.createDataFrame(
        [
            ("WalmartBrand", ["Walmart", "WhatIfAmazonBuysWalmart"]),
            ("WalmartOptics", ["WhatIfWalmartHadAcquiredOpticsFromTarget", "Walmart"]),
            ("SouthwestAirlinesInFlight", ["SouthwestAirlines"]),
            ("SouthwestAirlinesBrand", ["SouthwestAirlines"]),
            ("Veeba", ["SauceCompany"]),
        ],
        schema=[
            "brand_id",
            "company_ids",
        ],
    )
    # fmt: on

    result_df = filter_same_company_collisions(test_transitive_relationships_df, brand_id_to_company_ids_map).toPandas()

    assert len(result_df) == 1
    assert result_df[OUTPUT_FIELD_AFTER_TRANSITIVITY].values.tolist() == ["Veeba"]


def test_enrich_transitive_relationships_df(spark):
    # fmt off:
    test_transitive_relationships_df = spark.createDataFrame(
        [
            ("Walmart 123", 100, "WalmartBrand", "WalmartOptics"),
            ("Southwest Sauce", 200, "SouthwestAirlinesInFlight", "Veeba"),
            ("SW* Midair Sandwich", 400, "SouthwestAirlinesBrand", "SouthwestAirlinesInFlight"),
        ],
        schema=["dp4_name", "n_txns", TRANSITIVITY_OUTPUT_FIELD, OUTPUT_FIELD_AFTER_TRANSITIVITY],
    )
    # fmt: on

    # fmt off:
    output_field_to_n_txns = spark.createDataFrame(
        [
            ("WalmartBrand", 100000),
            ("WalmartOptics", 1000),
            ("SouthwestAirlinesInFlight", 2000),
            ("SouthwestAirlinesBrand", 2000),
            ("Veeba", 40),
        ],
        schema=[OUTPUT_FIELD_AFTER_TRANSITIVITY, f"{OUTPUT_FIELD_AFTER_TRANSITIVITY}_txns"],
    )
    # fmt: on

    result_df = enrich_transitive_relationships_df(test_transitive_relationships_df, output_field_to_n_txns).toPandas()

    assert len(result_df) == 3
    assert result_df[result_df[OUTPUT_FIELD_AFTER_TRANSITIVITY] == "WalmartOptics"][
        f"pct_{OUTPUT_FIELD_AFTER_TRANSITIVITY}_txns"
    ].to_list() == [10.0]
    assert result_df[result_df[OUTPUT_FIELD_AFTER_TRANSITIVITY] == "Veeba"][
        f"pct_{OUTPUT_FIELD_AFTER_TRANSITIVITY}_txns"
    ].to_list() == [500.0]
    assert result_df[result_df[OUTPUT_FIELD_AFTER_TRANSITIVITY] == "SouthwestAirlinesInFlight"][
        f"pct_{OUTPUT_FIELD_AFTER_TRANSITIVITY}_txns"
    ].to_list() == [20.0]
