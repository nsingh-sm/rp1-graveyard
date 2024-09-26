import logging
from datetime import datetime

from src.constants import (
    MAPPED_TXN_DESCRIPTORS_SCHEMA,
    RP1_MMHID_COL,
    DP4_DESCRIPTOR_COLS,
    DP4_DESCRIPTOR_NAME,
    DP4_TRIMMED_DESCRIPTOR_COLS,
    DP4_DESCRIPTOR_TRIMMED_NAME,
    DP4_DESCRIPTOR_TRIMMED_CITY,
    RP1_TRIMMED_DESCRIPTOR_COLS,
    RP1_DESCRIPTOR_TRIMMED_NAME,
    RP1_DESCRIPTOR_TRIMMED_CITY,
    VALID_FROM_COL,
    VALID_UNTIL_COL,
    JOIN_METHODOLOGY_COL,
    JOIN_MECHANISM_EXACT,
    JOIN_MECHANISM_STARTSWITH,
)

from src.join_methodologies import (
    _split_mapped_txns_df,
    _preprocess_city,
    _postprocess_city,
    exact_join,
    startswith_join,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def test_split_mapped_txns_df_successfully_mapped_txns_df(spark, test_split_mapped_txns_df_input_df):
    expected_successfully_mapped_txns = spark.createDataFrame(
        [
            (
                "Name_1",
                "State_1",
                "City_1",
                1,
                "Name_1",
                datetime(2020, 1, 1).date(),
                datetime(2021, 1, 1).date(),
                "join",
            ),
            (
                "Name_1 Trim ",
                " Trim State_1",
                " City_1   ",
                1,
                "Name_1",
                datetime(2020, 1, 1).date(),
                datetime(2021, 1, 1).date(),
                "join",
            ),
        ],
        schema=[
            *DP4_DESCRIPTOR_COLS,
            RP1_MMHID_COL,
            RP1_DESCRIPTOR_TRIMMED_NAME,
            VALID_FROM_COL,
            VALID_UNTIL_COL,
            JOIN_METHODOLOGY_COL,
        ],
    ).toPandas()

    successfully_mapped_txns, _ = _split_mapped_txns_df(test_split_mapped_txns_df_input_df, "join")
    successfully_mapped_txns = successfully_mapped_txns.toPandas()

    assert sorted(successfully_mapped_txns.columns.tolist()) == sorted(MAPPED_TXN_DESCRIPTORS_SCHEMA.fieldNames())
    assert len(successfully_mapped_txns) == 2

    successfully_mapped_txns = successfully_mapped_txns.sort_values(by=DP4_DESCRIPTOR_NAME).reset_index(drop=True)
    expected_successfully_mapped_txns = expected_successfully_mapped_txns.sort_values(
        by=DP4_DESCRIPTOR_NAME
    ).reset_index(drop=True)
    assert successfully_mapped_txns.equals(expected_successfully_mapped_txns)


def test_split_mapped_txns_df_remaining_txns_df(spark, test_split_mapped_txns_df_input_df):
    expected_remaining_txns = spark.createDataFrame(
        [
            ("Name_2", "State_2", "City_2", "Name_2", "State_2", "City_2"),
        ],
        schema=[*DP4_DESCRIPTOR_COLS, *DP4_TRIMMED_DESCRIPTOR_COLS],
    ).toPandas()

    _, remaining_txns = _split_mapped_txns_df(test_split_mapped_txns_df_input_df, "join")
    remaining_txns = remaining_txns.toPandas()

    assert sorted(remaining_txns.columns.tolist()) == sorted(DP4_DESCRIPTOR_COLS + DP4_TRIMMED_DESCRIPTOR_COLS)
    assert len(remaining_txns) == 1

    assert remaining_txns.equals(expected_remaining_txns)


def test_preprocess_city_negative_case(test_preprocess_city_input_txns_df, test_preprocess_city_input_descriptors_df):
    expected_txns_df_negative_case = test_preprocess_city_input_txns_df.toPandas()
    expected_descriptors_df_negative_case = test_preprocess_city_input_descriptors_df.toPandas()

    txns_df, descriptors_df = _preprocess_city(
        test_preprocess_city_input_txns_df, test_preprocess_city_input_descriptors_df, False
    )
    txns_df = txns_df.toPandas()
    descriptors_df = descriptors_df.toPandas()

    txns_df = txns_df.sort_values(by=DP4_DESCRIPTOR_TRIMMED_NAME).reset_index(drop=True)
    expected_txns_df_negative_case = expected_txns_df_negative_case.sort_values(
        by=DP4_DESCRIPTOR_TRIMMED_NAME
    ).reset_index(drop=True)
    assert txns_df.equals(expected_txns_df_negative_case)

    expected_descriptors_df_negative_case = expected_descriptors_df_negative_case.sort_values(
        by=RP1_DESCRIPTOR_TRIMMED_NAME
    ).reset_index(drop=True)
    descriptors_df = descriptors_df.sort_values(by=RP1_DESCRIPTOR_TRIMMED_NAME).reset_index(drop=True)
    assert descriptors_df.equals(expected_descriptors_df_negative_case)


def test_preprocess_city(spark, test_preprocess_city_input_txns_df, test_preprocess_city_input_descriptors_df):
    expected_txns_df = spark.createDataFrame(
        [
            ("Name_0", "State_0", "City0", "City0"),
            ("Name_1", "State_1", "City1", "City_1"),
            ("Name_2", "State_2", "City2", "City    2"),
            ("Name_3", "State_3", "City3", "City--3"),
        ],
        schema=DP4_TRIMMED_DESCRIPTOR_COLS + ["_" + DP4_DESCRIPTOR_TRIMMED_CITY],
    ).toPandas()

    expected_descriptors_df = spark.createDataFrame(
        [
            ("Name_00", "State_00", "City00", "C-i-t-y-00"),
            ("Name_11", "State_11", "City11", "C i..t//~ y11"),
        ],
        schema=RP1_TRIMMED_DESCRIPTOR_COLS + ["_" + RP1_DESCRIPTOR_TRIMMED_CITY],
    ).toPandas()

    txns_df, descriptors_df = _preprocess_city(
        test_preprocess_city_input_txns_df, test_preprocess_city_input_descriptors_df, True
    )
    txns_df = txns_df.toPandas()
    descriptors_df = descriptors_df.toPandas()

    txns_df = txns_df.sort_values(by=DP4_DESCRIPTOR_TRIMMED_NAME).reset_index(drop=True)
    expected_txns_df = expected_txns_df.sort_values(by=DP4_DESCRIPTOR_TRIMMED_NAME).reset_index(drop=True)
    assert txns_df.equals(expected_txns_df)

    descriptors_df = descriptors_df.sort_values(by=RP1_DESCRIPTOR_TRIMMED_NAME).reset_index(drop=True)
    expected_descriptors_df = expected_descriptors_df.sort_values(by=RP1_DESCRIPTOR_TRIMMED_NAME).reset_index(drop=True)
    assert descriptors_df.equals(expected_descriptors_df)


def test_postprocess_city_negative_casee(test_postprocess_city_input_mapped_txns_df):
    expected_mapped_txns_df_negative_case = test_postprocess_city_input_mapped_txns_df.toPandas()
    mapped_txns_df = _postprocess_city(test_postprocess_city_input_mapped_txns_df, False).toPandas()

    mapped_txns_df = mapped_txns_df.sort_values(by=DP4_DESCRIPTOR_TRIMMED_NAME).reset_index(drop=True)
    expected_mapped_txns_df_negative_case = expected_mapped_txns_df_negative_case.sort_values(
        by=DP4_DESCRIPTOR_TRIMMED_NAME
    ).reset_index(drop=True)
    assert mapped_txns_df.equals(expected_mapped_txns_df_negative_case)


def test_postprocess_city(spark, test_postprocess_city_input_mapped_txns_df):
    expected_mapped_txns_df_negative_case = test_postprocess_city_input_mapped_txns_df.toPandas()
    mapped_txns_df = _postprocess_city(test_postprocess_city_input_mapped_txns_df, False).toPandas()

    mapped_txns_df = mapped_txns_df.sort_values(by=DP4_DESCRIPTOR_TRIMMED_NAME).reset_index(drop=True)
    expected_mapped_txns_df_negative_case = expected_mapped_txns_df_negative_case.sort_values(
        by=DP4_DESCRIPTOR_TRIMMED_NAME
    ).reset_index(drop=True)
    assert mapped_txns_df.equals(expected_mapped_txns_df_negative_case)

    expected_mapped_txns_df = spark.createDataFrame(
        [
            ("Name_0", "State_0", "City0", 1),
            ("Name_1", "State_1", "City_1", 2),
            ("Name_2", "State_2", "City    2", 3),
            ("Name_3", "State_3", "City--3", 4),
        ],
        schema=DP4_TRIMMED_DESCRIPTOR_COLS + [RP1_MMHID_COL],
    ).toPandas()
    mapped_txns_df = _postprocess_city(test_postprocess_city_input_mapped_txns_df, True).toPandas()

    mapped_txns_df = mapped_txns_df.sort_values(by=DP4_DESCRIPTOR_TRIMMED_NAME).reset_index(drop=True)
    expected_mapped_txns_df = expected_mapped_txns_df.sort_values(by=DP4_DESCRIPTOR_TRIMMED_NAME).reset_index(drop=True)
    assert mapped_txns_df.equals(expected_mapped_txns_df)


def test_exact_join_basic_configuration(spark, test_exact_join_input_txns_df, test_exact_join_input_descriptors_df):
    successfully_mapped_txns, remaining_txns = exact_join(
        test_exact_join_input_txns_df, test_exact_join_input_descriptors_df, "test", True, False
    )
    successfully_mapped_txns = (
        successfully_mapped_txns.toPandas().sort_values(by=DP4_DESCRIPTOR_NAME).reset_index(drop=True)
    )
    remaining_txns = remaining_txns.toPandas().sort_values(by=DP4_DESCRIPTOR_NAME).reset_index(drop=True)

    expected_successfully_mapped_txns = spark.createDataFrame(
        [
            (
                "Name_0",
                "State_0",
                "City0",
                0,
                "Name_0",
                datetime(2000, 1, 1).date(),
                datetime(2100, 1, 1).date(),
                JOIN_MECHANISM_EXACT + "_test",
            ),
        ],
        schema=[
            *DP4_DESCRIPTOR_COLS,
            RP1_MMHID_COL,
            RP1_DESCRIPTOR_TRIMMED_NAME,
            VALID_FROM_COL,
            VALID_UNTIL_COL,
            JOIN_METHODOLOGY_COL,
        ],
    ).toPandas()

    successfully_mapped_txns = successfully_mapped_txns.sort_values(
        by=[DP4_DESCRIPTOR_NAME, RP1_MMHID_COL]
    ).reset_index(drop=True)
    expected_successfully_mapped_txns = expected_successfully_mapped_txns.sort_values(
        by=[DP4_DESCRIPTOR_NAME, RP1_MMHID_COL]
    ).reset_index(drop=True)
    assert successfully_mapped_txns.equals(expected_successfully_mapped_txns)

    expected_remaining_txns = spark.createDataFrame(
        [
            ("Name_1 ", "State_1 ", "City_1 ", "Name_1", "State_1", "City_1"),
            ("Name_2", " State_2", " City    2", "Name_2", "State_2", "City    2"),
            ("Name_3 ", " State_3 ", " City--3 ", "Name_3", "State_3", "City--3"),
        ],
        schema=DP4_DESCRIPTOR_COLS + DP4_TRIMMED_DESCRIPTOR_COLS,
    ).toPandas()

    remaining_txns = remaining_txns.sort_values(by=[DP4_DESCRIPTOR_NAME]).reset_index(drop=True)
    expected_remaining_txns = expected_remaining_txns.sort_values(by=[DP4_DESCRIPTOR_NAME]).reset_index(drop=True)
    assert remaining_txns.equals(expected_remaining_txns)


def test_exact_join_processed_city_configuration(
    spark, test_exact_join_input_txns_df, test_exact_join_input_descriptors_df
):
    successfully_mapped_txns, remaining_txns = exact_join(
        test_exact_join_input_txns_df, test_exact_join_input_descriptors_df, "test", True, True
    )
    successfully_mapped_txns = successfully_mapped_txns.toPandas()
    remaining_txns = remaining_txns.toPandas()

    expected_successfully_mapped_txns = spark.createDataFrame(
        [
            (
                "Name_0",
                "State_0",
                "City0",
                0,
                "Name_0",
                datetime(2000, 1, 1).date(),
                datetime(2100, 1, 1).date(),
                JOIN_MECHANISM_EXACT + "_test",
            ),
            (
                "Name_1 ",
                "State_1 ",
                "City_1 ",
                1,
                "Name_1",
                datetime(2000, 1, 1).date(),
                datetime(2018, 5, 24).date(),
                JOIN_MECHANISM_EXACT + "_test",
            ),
            (
                "Name_1 ",
                "State_1 ",
                "City_1 ",
                10,
                "Name_1",
                datetime(2018, 5, 25).date(),
                datetime(2100, 1, 1).date(),
                JOIN_MECHANISM_EXACT + "_test",
            ),
        ],
        schema=[
            *DP4_DESCRIPTOR_COLS,
            RP1_MMHID_COL,
            RP1_DESCRIPTOR_TRIMMED_NAME,
            VALID_FROM_COL,
            VALID_UNTIL_COL,
            JOIN_METHODOLOGY_COL,
        ],
    ).toPandas()

    successfully_mapped_txns = successfully_mapped_txns.sort_values(
        by=[DP4_DESCRIPTOR_NAME, RP1_MMHID_COL]
    ).reset_index(drop=True)
    expected_successfully_mapped_txns = expected_successfully_mapped_txns.sort_values(
        by=[DP4_DESCRIPTOR_NAME, RP1_MMHID_COL]
    ).reset_index(drop=True)
    assert successfully_mapped_txns.equals(expected_successfully_mapped_txns)

    expected_remaining_txns = spark.createDataFrame(
        [
            (
                "Name_2",
                " State_2",
                " City    2",
                "Name_2",
                "State_2",
                "City    2",
            ),
            ("Name_3 ", " State_3 ", " City--3 ", "Name_3", "State_3", "City--3"),
        ],
        schema=DP4_DESCRIPTOR_COLS + DP4_TRIMMED_DESCRIPTOR_COLS,
    ).toPandas()

    remaining_txns = remaining_txns.sort_values(by=[DP4_DESCRIPTOR_NAME]).reset_index(drop=True)
    expected_remaining_txns = expected_remaining_txns.sort_values(by=[DP4_DESCRIPTOR_NAME]).reset_index(drop=True)
    assert remaining_txns.equals(expected_remaining_txns)


def test_exact_join_without_city_configuration(
    spark, test_exact_join_input_txns_df, test_exact_join_input_descriptors_df
):
    successfully_mapped_txns, remaining_txns = exact_join(
        test_exact_join_input_txns_df, test_exact_join_input_descriptors_df, "test", False, False
    )
    successfully_mapped_txns = successfully_mapped_txns.toPandas()
    remaining_txns = remaining_txns.toPandas()

    expected_successfully_mapped_txns = spark.createDataFrame(
        [
            (
                "Name_0",
                "State_0",
                "City0",
                0,
                "Name_0",
                datetime(2000, 1, 1).date(),
                datetime(2100, 1, 1).date(),
                JOIN_MECHANISM_EXACT + "_test",
            ),
            (
                "Name_1 ",
                "State_1 ",
                "City_1 ",
                1,
                "Name_1",
                datetime(2000, 1, 1).date(),
                datetime(2018, 5, 24).date(),
                JOIN_MECHANISM_EXACT + "_test",
            ),
            (
                "Name_1 ",
                "State_1 ",
                "City_1 ",
                10,
                "Name_1",
                datetime(2018, 5, 25).date(),
                datetime(2100, 1, 1).date(),
                JOIN_MECHANISM_EXACT + "_test",
            ),
            (
                "Name_2",
                " State_2",
                " City    2",
                2,
                "Name_2",
                datetime(2000, 1, 1).date(),
                datetime(2100, 1, 1).date(),
                JOIN_MECHANISM_EXACT + "_test",
            ),
        ],
        schema=[
            *DP4_DESCRIPTOR_COLS,
            RP1_MMHID_COL,
            RP1_DESCRIPTOR_TRIMMED_NAME,
            VALID_FROM_COL,
            VALID_UNTIL_COL,
            JOIN_METHODOLOGY_COL,
        ],
    ).toPandas()

    successfully_mapped_txns = successfully_mapped_txns.sort_values(
        by=[DP4_DESCRIPTOR_NAME, RP1_MMHID_COL]
    ).reset_index(drop=True)
    expected_successfully_mapped_txns = expected_successfully_mapped_txns.sort_values(
        by=[DP4_DESCRIPTOR_NAME, RP1_MMHID_COL]
    ).reset_index(drop=True)
    assert successfully_mapped_txns.equals(expected_successfully_mapped_txns)

    expected_remaining_txns = spark.createDataFrame(
        [
            ("Name_3 ", " State_3 ", " City--3 ", "Name_3", "State_3", "City--3"),
        ],
        schema=DP4_DESCRIPTOR_COLS + DP4_TRIMMED_DESCRIPTOR_COLS,
    ).toPandas()

    remaining_txns = remaining_txns.sort_values(by=[DP4_DESCRIPTOR_NAME]).reset_index(drop=True)
    expected_remaining_txns = expected_remaining_txns.sort_values(by=[DP4_DESCRIPTOR_NAME]).reset_index(drop=True)
    assert remaining_txns.equals(expected_remaining_txns)


def test_startswith_join_basic_configuration(
    spark, test_startswith_join_input_txns_df, test_startswith_join_input_descriptors_df
):
    successfully_mapped_txns, remaining_txns = startswith_join(
        test_startswith_join_input_txns_df, test_startswith_join_input_descriptors_df, "test", True, False
    )
    successfully_mapped_txns = successfully_mapped_txns.toPandas()
    remaining_txns = remaining_txns.toPandas()

    expected_successfully_mapped_txns = spark.createDataFrame(
        [
            (
                "1234567890_Name_0",
                "State_0",
                "City0",
                100,
                "1234567890_Name_00",
                datetime(2000, 1, 1).date(),
                datetime(2100, 1, 1).date(),
                JOIN_MECHANISM_STARTSWITH + "_test",
            ),
            (
                "1234567890_Name_0",
                "State_0",
                "City0",
                1000,
                "1234567890_Name_000",
                datetime(2000, 1, 1).date(),
                datetime(2100, 1, 1).date(),
                JOIN_MECHANISM_STARTSWITH + "_test",
            ),
        ],
        schema=[
            *DP4_DESCRIPTOR_COLS,
            RP1_MMHID_COL,
            RP1_DESCRIPTOR_TRIMMED_NAME,
            VALID_FROM_COL,
            VALID_UNTIL_COL,
            JOIN_METHODOLOGY_COL,
        ],
    ).toPandas()

    successfully_mapped_txns = successfully_mapped_txns.sort_values(
        by=[DP4_DESCRIPTOR_NAME, RP1_MMHID_COL]
    ).reset_index(drop=True)
    expected_successfully_mapped_txns = expected_successfully_mapped_txns.sort_values(
        by=[DP4_DESCRIPTOR_NAME, RP1_MMHID_COL]
    ).reset_index(drop=True)
    assert successfully_mapped_txns.equals(expected_successfully_mapped_txns)

    expected_remaining_txns = spark.createDataFrame(
        [
            ("1234567890_Name_1 ", "State_1 ", "City_1 ", "1234567890_Name_1", "State_1", "City_1"),
            (
                "1234567890_Name_2",
                " State_2",
                " City    2",
                "1234567890_Name_2",
                "State_2",
                "City    2",
            ),
            ("1234567890_Name_3 ", " State_3 ", " City--3 ", "1234567890_Name_3", "State_3", "City--3"),
            ("Name_0", "State_0", "City0", "Name_0", "State_0", "City0"),
        ],
        schema=DP4_DESCRIPTOR_COLS + DP4_TRIMMED_DESCRIPTOR_COLS,
    ).toPandas()

    remaining_txns = remaining_txns.sort_values(by=[DP4_DESCRIPTOR_NAME]).reset_index(drop=True)
    expected_remaining_txns = expected_remaining_txns.sort_values(by=[DP4_DESCRIPTOR_NAME]).reset_index(drop=True)
    assert remaining_txns.equals(expected_remaining_txns)


def test_startswith_join_processed_city_configuration(
    spark, test_startswith_join_input_txns_df, test_startswith_join_input_descriptors_df
):
    successfully_mapped_txns, remaining_txns = startswith_join(
        test_startswith_join_input_txns_df, test_startswith_join_input_descriptors_df, "test", True, True
    )
    successfully_mapped_txns = successfully_mapped_txns.toPandas()
    remaining_txns = remaining_txns.toPandas()

    expected_successfully_mapped_txns = spark.createDataFrame(
        [
            (
                "1234567890_Name_0",
                "State_0",
                "City0",
                100,
                "1234567890_Name_00",
                datetime(2000, 1, 1).date(),
                datetime(2100, 1, 1).date(),
                JOIN_MECHANISM_STARTSWITH + "_test",
            ),
            (
                "1234567890_Name_0",
                "State_0",
                "City0",
                1000,
                "1234567890_Name_000",
                datetime(2000, 1, 1).date(),
                datetime(2100, 1, 1).date(),
                JOIN_MECHANISM_STARTSWITH + "_test",
            ),
            (
                "1234567890_Name_1 ",
                "State_1 ",
                "City_1 ",
                10,
                "1234567890_Name_10",
                datetime(2000, 1, 1).date(),
                datetime(2018, 5, 24).date(),
                JOIN_MECHANISM_STARTSWITH + "_test",
            ),
            (
                "1234567890_Name_1 ",
                "State_1 ",
                "City_1 ",
                11,
                "1234567890_Name_10",
                datetime(2018, 5, 25).date(),
                datetime(2100, 1, 1).date(),
                JOIN_MECHANISM_STARTSWITH + "_test",
            ),
        ],
        schema=[
            *DP4_DESCRIPTOR_COLS,
            RP1_MMHID_COL,
            RP1_DESCRIPTOR_TRIMMED_NAME,
            VALID_FROM_COL,
            VALID_UNTIL_COL,
            JOIN_METHODOLOGY_COL,
        ],
    ).toPandas()

    successfully_mapped_txns = successfully_mapped_txns.sort_values(
        by=[DP4_DESCRIPTOR_NAME, RP1_MMHID_COL]
    ).reset_index(drop=True)
    expected_successfully_mapped_txns = expected_successfully_mapped_txns.sort_values(
        by=[DP4_DESCRIPTOR_NAME, RP1_MMHID_COL]
    ).reset_index(drop=True)
    assert successfully_mapped_txns.equals(expected_successfully_mapped_txns)

    expected_remaining_txns = spark.createDataFrame(
        [
            (
                "1234567890_Name_2",
                " State_2",
                " City    2",
                "1234567890_Name_2",
                "State_2",
                "City    2",
            ),
            ("1234567890_Name_3 ", " State_3 ", " City--3 ", "1234567890_Name_3", "State_3", "City--3"),
            ("Name_0", "State_0", "City0", "Name_0", "State_0", "City0"),
        ],
        schema=DP4_DESCRIPTOR_COLS + DP4_TRIMMED_DESCRIPTOR_COLS,
    ).toPandas()

    remaining_txns = remaining_txns.sort_values(by=[DP4_DESCRIPTOR_NAME]).reset_index(drop=True)
    expected_remaining_txns = expected_remaining_txns.sort_values(by=[DP4_DESCRIPTOR_NAME]).reset_index(drop=True)
    assert remaining_txns.equals(expected_remaining_txns)


def test_startswith_join_without_city_configuration(
    spark, test_startswith_join_input_txns_df, test_startswith_join_input_descriptors_df
):
    successfully_mapped_txns, remaining_txns = startswith_join(
        test_startswith_join_input_txns_df, test_startswith_join_input_descriptors_df, "test", False, False
    )
    successfully_mapped_txns = successfully_mapped_txns.toPandas()
    remaining_txns = remaining_txns.toPandas()

    expected_successfully_mapped_txns = spark.createDataFrame(
        [
            (
                "1234567890_Name_0",
                "State_0",
                "City0",
                100,
                "1234567890_Name_00",
                datetime(2000, 1, 1).date(),
                datetime(2100, 1, 1).date(),
                JOIN_MECHANISM_STARTSWITH + "_test",
            ),
            (
                "1234567890_Name_0",
                "State_0",
                "City0",
                1000,
                "1234567890_Name_000",
                datetime(2000, 1, 1).date(),
                datetime(2100, 1, 1).date(),
                JOIN_MECHANISM_STARTSWITH + "_test",
            ),
            (
                "1234567890_Name_1 ",
                "State_1 ",
                "City_1 ",
                10,
                "1234567890_Name_10",
                datetime(2000, 1, 1).date(),
                datetime(2018, 5, 24).date(),
                JOIN_MECHANISM_STARTSWITH + "_test",
            ),
            (
                "1234567890_Name_1 ",
                "State_1 ",
                "City_1 ",
                11,
                "1234567890_Name_10",
                datetime(2018, 5, 25).date(),
                datetime(2100, 1, 1).date(),
                JOIN_MECHANISM_STARTSWITH + "_test",
            ),
            (
                "1234567890_Name_2",
                " State_2",
                " City    2",
                20,
                "1234567890_Name_20",
                datetime(2000, 1, 1).date(),
                datetime(2100, 1, 1).date(),
                JOIN_MECHANISM_STARTSWITH + "_test",
            ),
        ],
        schema=[
            *DP4_DESCRIPTOR_COLS,
            RP1_MMHID_COL,
            RP1_DESCRIPTOR_TRIMMED_NAME,
            VALID_FROM_COL,
            VALID_UNTIL_COL,
            JOIN_METHODOLOGY_COL,
        ],
    ).toPandas()

    successfully_mapped_txns = successfully_mapped_txns.sort_values(
        by=[DP4_DESCRIPTOR_NAME, RP1_MMHID_COL]
    ).reset_index(drop=True)

    expected_successfully_mapped_txns = expected_successfully_mapped_txns.sort_values(
        by=[DP4_DESCRIPTOR_NAME, RP1_MMHID_COL]
    ).reset_index(drop=True)
    assert successfully_mapped_txns.equals(expected_successfully_mapped_txns)

    expected_remaining_txns = spark.createDataFrame(
        [
            ("1234567890_Name_3 ", " State_3 ", " City--3 ", "1234567890_Name_3", "State_3", "City--3"),
            (
                "Name_0",
                "State_0",
                "City0",
                "Name_0",
                "State_0",
                "City0",
            ),
        ],
        schema=DP4_DESCRIPTOR_COLS + DP4_TRIMMED_DESCRIPTOR_COLS,
    ).toPandas()

    remaining_txns = remaining_txns.sort_values(by=[DP4_DESCRIPTOR_NAME]).reset_index(drop=True)
    expected_remaining_txns = expected_remaining_txns.sort_values(by=[DP4_DESCRIPTOR_NAME]).reset_index(drop=True)
    assert remaining_txns.equals(expected_remaining_txns)
