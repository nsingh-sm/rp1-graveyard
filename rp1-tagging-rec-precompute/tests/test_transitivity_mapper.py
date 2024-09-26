from src.transitivity_mapper import (
    filter_bad_feature_output_relationships,
    get_features_to_output_relationships,
)

from src.constants import TRANSITIVITY_OUTPUT_FIELD, OUTPUT_FIELD_AFTER_TRANSITIVITY


def test_filter_bad_feature_output_relationships(spark):
    # fmt: off
    input_df = spark.createDataFrame(
        [
            ("Coke",                      "5600", "Cola",  30000, "12",     "Coca Cola", "Coca Cola"),
            ("Coke Soft Drink",           "5600", "Cola",  50000, "12||14", "Coca Cola", "Coca Cola"),
            ("C0ke",                      "5600", "NULL",  500,   "12",     "NON-AGGREGATED RESTAURANTS 5812", "NON-AGGREGATED"),
            ("C0ke",                      "5600", "NULL",  500,   "12",     "Coke||Coca Cola", "Coke||Coca Cola"),
            ("Fake Pepsi, actually C0ke", "5599", "Pepsi", 700,   "12",     None, None),
        ],
        schema=["dp4_name", "dp4_mcc", "bsm_brand", "n_txns", "rp1_mmhid", "clean_aggregate_merchant_name", "clean_parent_aggregate_merchant_name"],
    )
    # fmt: on

    assert filter_bad_feature_output_relationships(input_df, ["rp1_mmhid"]).count() == 4
    assert filter_bad_feature_output_relationships(input_df, ["clean_aggregate_merchant_name"]).count() == 2


def test_get_features_to_output_relationships(spark):
    # fmt: off
    test_input_txns_df = spark.createDataFrame(
        [
            ("Coke",                      "Kansas City",     "5600", "Cola",     30000, "Coke",                       "12", "Coca Cola"),
            ("Coke Soft Drink",           "Kansas City",     "5600", "Cola",     50000, "Coke Soft Drink Too",        "12", "Coca Cola"),
            ("C0ke",                      "Kansas City",     "5600", "NULL",     500,   "C0ke with 0",                "12", "Coca Cola"),  # Transitivity using MCC and MMHID
            ("Fake Pepsi, actually C0ke", "(888) 555-0123",  "5599", "Pepsi",    700,   "Fake Pepsi, actually C0ke",  "12", "Coca Cola"),  # Transitivity using MMHID
            ("Another c0ke Merchant",     "Cleveland",       "5600", "NULL",     400,   "Another c0ke Merchant haha", "13", "Coca Cola"),  # Transitivity using Aggregate Merchant Name
            ("Fake Pepsi, actually",      "Queens",          "5300", "Pepsi",    600,   "Fake Pepsi, actually C0ke",  "12", "Coca Cola"),  # Bad record, spoils transitivity using MMHID and Aggregate Merchant Name
            ("Pepsi Store",               "Portland",        "5300", "Pepsi",    40000, "Pepsi Store",                "5",  "Pepsi"),
            ("Pepsi Sto",                 "Oakland",         "5301", "Pepsi",    20000, "Pepsi Store",                "5",  "Pepsi"),
            ("Ppsi Store",                "Bad Record",      "5300", "NULL",     400,   "Ppsi Store",                 "5",  "Pepsi"),      # Transitivity using MMHID
            ("Gatorade Shop",             "Fort Lauderdale", "5602", "Gatorade", 10000, "Gatorade Shop",              "47", "Gatorade"),   # Harmless records
            ("Fanta Outlet",              "Ithaca",          "5603", "NULL",     1000,  "Fanta Outlet",               "58", "Fanta"),      # Harmless records
            ("Dr. Pepper Merchant",       "Atlanta",         "5600", "NULL",     2000,  "Dr. Pepper Merchant",        "78", "Dr. Pepper"), # No transitivity
            ("Dr. Pepper Merch",          "Atlanta",         "5600", "NULL",     1000,  "Dr. Pepper Merchant",        "78", "Dr. Pepper"), # No transitivity
        ],
        schema=["dp4_name", "dp4_city", "dp4_mcc", TRANSITIVITY_OUTPUT_FIELD, "n_txns", "rp1_name", "rp1_mmhid", "rp1_amn"],
    )
    # fmt: on

    transitivity_using_mcc = get_features_to_output_relationships(test_input_txns_df, ["rp1_mmhid"]).toPandas()

    # +---------+----------------------------------+---------------------+-------------------------+-------------------+
    # |rp1_mmhid|transitive_vendor_primary_merchant|transitivity_strength|transitivity_distribution|transitive_features|
    # +---------+----------------------------------+---------------------+-------------------------+-------------------+
    # |       47|                          Gatorade|                100.0|     [{"Gatorade":100.0}]|          rp1_mmhid|
    # |        5|                             Pepsi|                99.34|        [{"Pepsi":99.34}]|          rp1_mmhid|
    # |       12|                              Cola|                 97.8|          [{"Cola":97.8}]|          rp1_mmhid|
    # +---------+----------------------------------+---------------------+-------------------------+-------------------+

    assert len(transitivity_using_mcc) == 3

    for mmhid in ["5", "12", "47"]:
        assert mmhid in transitivity_using_mcc["rp1_mmhid"].tolist()

    for transitive_vpm in ["Gatorade", "Cola", "Pepsi"]:
        assert transitive_vpm in transitivity_using_mcc[OUTPUT_FIELD_AFTER_TRANSITIVITY].tolist()

    assert (
        transitivity_using_mcc[transitivity_using_mcc["rp1_mmhid"] == "47"]["transitivity_strength"].tolist()[0] == 100
    )

    transitivity_using_mcc_and_mmhid = get_features_to_output_relationships(
        test_input_txns_df, ["dp4_mcc", "rp1_mmhid"]
    ).toPandas()

    # +-------+---------+----------------------------------+---------------------+-------------------------+-------------------+
    # |dp4_mcc|rp1_mmhid|transitive_vendor_primary_merchant|transitivity_strength|transitivity_distribution|transitive_features|
    # +-------+---------+----------------------------------+---------------------+-------------------------+-------------------+
    # |   5300|       12|                             Pepsi|                100.0|        [{"Pepsi":100.0}]| dp4_mcc, rp1_mmhid|
    # |   5600|       12|                              Cola|                99.38|         [{"Cola":99.38}]| dp4_mcc, rp1_mmhid|
    # |   5602|       47|                          Gatorade|                100.0|     [{"Gatorade":100.0}]| dp4_mcc, rp1_mmhid|
    # |   5599|       12|                             Pepsi|                100.0|        [{"Pepsi":100.0}]| dp4_mcc, rp1_mmhid|
    # |   5300|        5|                             Pepsi|                99.01|        [{"Pepsi":99.01}]| dp4_mcc, rp1_mmhid|
    # |   5301|        5|                             Pepsi|                100.0|        [{"Pepsi":100.0}]| dp4_mcc, rp1_mmhid|
    # +-------+---------+----------------------------------+---------------------+-------------------------+-------------------+

    assert len(transitivity_using_mcc_and_mmhid) == 6

    assert len(transitivity_using_mcc_and_mmhid[transitivity_using_mcc_and_mmhid["dp4_mcc"] == "5300"]) == 2
    assert len(transitivity_using_mcc_and_mmhid[transitivity_using_mcc_and_mmhid["dp4_mcc"] == "5301"]) == 1
    assert len(transitivity_using_mcc_and_mmhid[transitivity_using_mcc_and_mmhid["dp4_mcc"] == "5599"]) == 1
    assert len(transitivity_using_mcc_and_mmhid[transitivity_using_mcc_and_mmhid["dp4_mcc"] == "5600"]) == 1
    assert len(transitivity_using_mcc_and_mmhid[transitivity_using_mcc_and_mmhid["dp4_mcc"] == "5602"]) == 1

    assert len(transitivity_using_mcc_and_mmhid[transitivity_using_mcc_and_mmhid["rp1_mmhid"] == "5"]) == 2
    assert len(transitivity_using_mcc_and_mmhid[transitivity_using_mcc_and_mmhid["rp1_mmhid"] == "12"]) == 3
    assert len(transitivity_using_mcc_and_mmhid[transitivity_using_mcc_and_mmhid["rp1_mmhid"] == "47"]) == 1

    assert (
        len(
            transitivity_using_mcc_and_mmhid[
                transitivity_using_mcc_and_mmhid["transitive_features"] != "dp4_mcc, rp1_mmhid"
            ]
        )
        == 0
    )

    assert (
        len(
            transitivity_using_mcc_and_mmhid[
                transitivity_using_mcc_and_mmhid[OUTPUT_FIELD_AFTER_TRANSITIVITY] == "Gatorade"
            ]
        )
        == 1
    )
    assert (
        len(
            transitivity_using_mcc_and_mmhid[
                transitivity_using_mcc_and_mmhid[OUTPUT_FIELD_AFTER_TRANSITIVITY] == "Cola"
            ]
        )
        == 1
    )
    assert (
        len(
            transitivity_using_mcc_and_mmhid[
                transitivity_using_mcc_and_mmhid[OUTPUT_FIELD_AFTER_TRANSITIVITY] == "Pepsi"
            ]
        )
        == 4
    )
