from src.compression_tools import compress_table, create_hash_columns
from datetime import datetime


def test_compress_table_without_fill_gaps(compress_table_input_df):
    """
    Expected result_wo_fill_gaps:
    +-----+-----+----------+----------+----------+----------+---------------+---------+--------+----------+
    |col_1|col_2|	  start|       end|start_date|  end_date|rows_compressed|day_range|gap_past|gap_future|
    +-----+-----+----------+----------+----------+----------+---------------+---------+--------+----------+
    |    x|	   y|2019-12-01|2020-02-29|2019-12-01|2020-02-29|              1|       91|     NaN|       0.0|
    |    x|	   Y|2020-03-01|2020-05-31|2020-03-01|2020-05-31|              1|       92|     0.0|       NaN|
    |    m|	   n|2019-12-01|2020-02-29|2019-12-01|2020-02-29|              1|       91|     NaN|       NaN|
    |    a|    b|2020-03-01|2020-05-31|2019-12-01|2020-05-31|              2|      183|     NaN|      92.0|
    |    a|    b|2020-09-01|2020-11-30|2020-09-01|2020-11-30|              1|       91|    92.0|      90.0|
    |    a|	 BBB|2021-03-01|2021-05-31|2021-03-01|2021-05-31|              1|       92|    90.0|       0.0|
    |    a|	   b|2021-06-01|2021-08-31|2021-06-01|2021-08-31|              1|       92|     0.0|       NaN|
    +-----+-----+----------+----------+----------+----------+---------------+---------+--------+----------+
    """
    hash_df = create_hash_columns(compress_table_input_df, primary_keys=["col_1"], data_columns=["col_2"])

    result_wo_fill_gaps = (
        compress_table(hash_df, ["col_1"], "data_key_hash", start_date_col="start", end_date_col="end")
        .drop("primary_key_hash")
        .drop("data_key_hash")
        .drop("comp_frac")
        .fillna(0.0)
        .toPandas()
    )

    assert sum(result_wo_fill_gaps["col_1"] == "x") == 2
    assert sum(result_wo_fill_gaps["col_1"] == "m") == 1
    assert sum(result_wo_fill_gaps["col_1"] == "a") == 4

    assert sum(result_wo_fill_gaps["col_2"] == "y") == 1
    assert sum(result_wo_fill_gaps["col_2"] == "Y") == 1
    assert sum(result_wo_fill_gaps["col_2"] == "n") == 1
    assert sum(result_wo_fill_gaps["col_2"] == "b") == 3
    assert sum(result_wo_fill_gaps["col_2"] == "BBB") == 1

    assert sum(result_wo_fill_gaps["rows_compressed"] == 1) == 6
    assert sum(result_wo_fill_gaps["rows_compressed"] == 2) == 1

    assert sum(result_wo_fill_gaps["day_range"] == 91) == 3
    assert sum(result_wo_fill_gaps["day_range"] == 92) == 3
    assert sum(result_wo_fill_gaps["day_range"] == 183) == 1

    assert sum(result_wo_fill_gaps["gap_past"] == 0) == 5
    assert sum(result_wo_fill_gaps["gap_past"] == 90) == 1
    assert sum(result_wo_fill_gaps["gap_past"] == 92) == 1

    assert sum(result_wo_fill_gaps["gap_future"] == 0) == 5
    assert sum(result_wo_fill_gaps["gap_future"] == 90) == 1
    assert sum(result_wo_fill_gaps["gap_future"] == 92) == 1

    assert result_wo_fill_gaps[(result_wo_fill_gaps["col_1"] == "x") & (result_wo_fill_gaps["col_2"] == "y")][
        "start_date"
    ].to_list() == [datetime(2019, 12, 1).date()]
    assert result_wo_fill_gaps[(result_wo_fill_gaps["col_1"] == "x") & (result_wo_fill_gaps["col_2"] == "y")][
        "end_date"
    ].to_list() == [datetime(2020, 2, 29).date()]

    assert result_wo_fill_gaps[(result_wo_fill_gaps["col_1"] == "x") & (result_wo_fill_gaps["col_2"] == "Y")][
        "start_date"
    ].to_list() == [datetime(2020, 3, 1).date()]
    assert result_wo_fill_gaps[(result_wo_fill_gaps["col_1"] == "x") & (result_wo_fill_gaps["col_2"] == "Y")][
        "end_date"
    ].to_list() == [datetime(2020, 5, 31).date()]

    assert result_wo_fill_gaps[(result_wo_fill_gaps["col_1"] == "m") & (result_wo_fill_gaps["col_2"] == "n")][
        "start_date"
    ].to_list() == [datetime(2019, 12, 1).date()]
    assert result_wo_fill_gaps[(result_wo_fill_gaps["col_1"] == "m") & (result_wo_fill_gaps["col_2"] == "n")][
        "end_date"
    ].to_list() == [datetime(2020, 2, 29).date()]

    assert sorted(
        result_wo_fill_gaps[(result_wo_fill_gaps["col_1"] == "a") & (result_wo_fill_gaps["col_2"] == "b")][
            "start_date"
        ].to_list()
    ) == [
        datetime(2019, 12, 1).date(),
        datetime(2020, 9, 1).date(),
        datetime(2021, 6, 1).date(),
    ]
    assert sorted(
        result_wo_fill_gaps[(result_wo_fill_gaps["col_1"] == "a") & (result_wo_fill_gaps["col_2"] == "b")][
            "end_date"
        ].to_list()
    ) == [
        datetime(2020, 5, 31).date(),
        datetime(2020, 11, 30).date(),
        datetime(2021, 8, 31).date(),
    ]


def test_compress_table_with_fill_gaps(compress_table_input_df):
    """
    Expected result_w_fill_gaps_true:
    +-----+-----+----------+----------+----------+----------+---------------+---------+--------+----------+
    |col_1|col_2|     start|       end|start_date|  end_date|rows_compressed|day_range|gap_past|gap_future|
    +-----+-----+----------+----------+----------+----------+---------------+---------+--------+----------+
    |    x|    y|2019-12-01|2020-02-29|2019-12-01|2020-02-29|              1|       91|    null|         0|
    |    x|    Y|2020-03-01|2020-05-31|2020-03-01|2020-05-31|              1|       92|       0|      null|
    |    m|    n|2019-12-01|2020-02-29|2019-12-01|2020-02-29|              1|       91|    null|      null|
    |    a|    b|2020-09-01|2020-11-30|2019-12-01|2020-11-30|              3|      366|    null|        90|
    |    a|  BBB|2021-03-01|2021-05-31|2021-03-01|2021-05-31|              1|       92|      90|         0|
    |    a|    b|2021-06-01|2021-08-31|2021-06-01|2021-08-31|              1|       92|       0|      null|
    +-----+-----+----------+----------+----------+----------+---------------+---------+--------+----------+
    """
    hash_df = create_hash_columns(compress_table_input_df, primary_keys=["col_1"], data_columns=["col_2"])

    result_w_fill_gaps_true = (
        compress_table(hash_df, ["col_1"], "data_key_hash", start_date_col="start", end_date_col="end", fill_gaps=True)
        .drop("primary_key_hash")
        .drop("data_key_hash")
        .drop("comp_frac")
        .fillna(0.0)
        .toPandas()
    )

    assert sum(result_w_fill_gaps_true["col_1"] == "x") == 2
    assert sum(result_w_fill_gaps_true["col_1"] == "m") == 1
    assert sum(result_w_fill_gaps_true["col_1"] == "a") == 3

    assert sum(result_w_fill_gaps_true["col_2"] == "y") == 1
    assert sum(result_w_fill_gaps_true["col_2"] == "Y") == 1
    assert sum(result_w_fill_gaps_true["col_2"] == "n") == 1
    assert sum(result_w_fill_gaps_true["col_2"] == "b") == 2
    assert sum(result_w_fill_gaps_true["col_2"] == "BBB") == 1

    assert sum(result_w_fill_gaps_true["rows_compressed"] == 1) == 5
    assert sum(result_w_fill_gaps_true["rows_compressed"] == 3) == 1

    assert sum(result_w_fill_gaps_true["day_range"] == 91) == 2
    assert sum(result_w_fill_gaps_true["day_range"] == 92) == 3
    assert sum(result_w_fill_gaps_true["day_range"] == 366) == 1

    assert sum(result_w_fill_gaps_true["gap_past"] == 0) == 5
    assert sum(result_w_fill_gaps_true["gap_past"] == 90) == 1

    assert sum(result_w_fill_gaps_true["gap_future"] == 0) == 5
    assert sum(result_w_fill_gaps_true["gap_future"] == 90) == 1

    assert result_w_fill_gaps_true[
        (result_w_fill_gaps_true["col_1"] == "x") & (result_w_fill_gaps_true["col_2"] == "y")
    ]["start_date"].to_list() == [datetime(2019, 12, 1).date()]
    assert result_w_fill_gaps_true[
        (result_w_fill_gaps_true["col_1"] == "x") & (result_w_fill_gaps_true["col_2"] == "y")
    ]["end_date"].to_list() == [datetime(2020, 2, 29).date()]

    assert result_w_fill_gaps_true[
        (result_w_fill_gaps_true["col_1"] == "x") & (result_w_fill_gaps_true["col_2"] == "Y")
    ]["start_date"].to_list() == [datetime(2020, 3, 1).date()]
    assert result_w_fill_gaps_true[
        (result_w_fill_gaps_true["col_1"] == "x") & (result_w_fill_gaps_true["col_2"] == "Y")
    ]["end_date"].to_list() == [datetime(2020, 5, 31).date()]

    assert result_w_fill_gaps_true[
        (result_w_fill_gaps_true["col_1"] == "m") & (result_w_fill_gaps_true["col_2"] == "n")
    ]["start_date"].to_list() == [datetime(2019, 12, 1).date()]
    assert result_w_fill_gaps_true[
        (result_w_fill_gaps_true["col_1"] == "m") & (result_w_fill_gaps_true["col_2"] == "n")
    ]["end_date"].to_list() == [datetime(2020, 2, 29).date()]

    assert sorted(
        result_w_fill_gaps_true[(result_w_fill_gaps_true["col_1"] == "a") & (result_w_fill_gaps_true["col_2"] == "b")][
            "start_date"
        ].to_list()
    ) == [
        datetime(2019, 12, 1).date(),
        datetime(2021, 6, 1).date(),
    ]

    assert sorted(
        result_w_fill_gaps_true[(result_w_fill_gaps_true["col_1"] == "a") & (result_w_fill_gaps_true["col_2"] == "b")][
            "end_date"
        ].to_list()
    ) == [
        datetime(2020, 11, 30).date(),
        datetime(2021, 8, 31).date(),
    ]


def test_compress_table_with_ffil_gaps(compress_table_input_df):
    """
    +-----+-----+----------+----------+----------+----------+---------------+---------+--------+----------+
    |col_1|col_2|     start|       end|start_date|  end_date|rows_compressed|day_range|gap_past|gap_future|
    +-----+-----+----------+----------+----------+----------+---------------+---------+--------+----------+
    |    x|    y|2019-12-01|2020-02-29|2019-12-01|2020-02-29|              1|       91|    null|         0|
    |    x|    Y|2020-03-01|2020-05-31|2020-03-01|2020-05-31|              1|       92|       0|      null|
    |    m|    n|2019-12-01|2020-02-29|2019-12-01|2020-02-29|              1|       91|    null|      null|
    |    a|    b|2020-09-01|2020-11-30|2019-12-01|2021-02-28|              3|      456|    null|         0|
    |    a|  BBB|2021-03-01|2021-05-31|2021-03-01|2021-05-31|              1|       92|       0|         0|
    |    a|    b|2021-06-01|2021-08-31|2021-06-01|2021-08-31|              1|       92|       0|      null|
    +-----+-----+----------+----------+----------+----------+---------------+---------+--------+----------+
    """
    hash_df = create_hash_columns(compress_table_input_df, primary_keys=["col_1"], data_columns=["col_2"])

    result_w_ffil_gaps_true = (
        compress_table(hash_df, ["col_1"], "data_key_hash", start_date_col="start", end_date_col="end", ffil_gaps=True)
        .drop("primary_key_hash")
        .drop("data_key_hash")
        .drop("comp_frac")
        .fillna(0.0)
        .toPandas()
    )

    assert sum(result_w_ffil_gaps_true["col_1"] == "x") == 2
    assert sum(result_w_ffil_gaps_true["col_1"] == "m") == 1
    assert sum(result_w_ffil_gaps_true["col_1"] == "a") == 3

    assert sum(result_w_ffil_gaps_true["col_2"] == "y") == 1
    assert sum(result_w_ffil_gaps_true["col_2"] == "Y") == 1
    assert sum(result_w_ffil_gaps_true["col_2"] == "n") == 1
    assert sum(result_w_ffil_gaps_true["col_2"] == "b") == 2
    assert sum(result_w_ffil_gaps_true["col_2"] == "BBB") == 1

    assert sum(result_w_ffil_gaps_true["rows_compressed"] == 1) == 5
    assert sum(result_w_ffil_gaps_true["rows_compressed"] == 3) == 1

    assert sum(result_w_ffil_gaps_true["day_range"] == 91) == 2
    assert sum(result_w_ffil_gaps_true["day_range"] == 92) == 3
    assert sum(result_w_ffil_gaps_true["day_range"] == 456) == 1

    assert sum(result_w_ffil_gaps_true["gap_past"] == 0) == 6
    assert sum(result_w_ffil_gaps_true["gap_future"] == 0) == 6

    assert result_w_ffil_gaps_true[
        (result_w_ffil_gaps_true["col_1"] == "x") & (result_w_ffil_gaps_true["col_2"] == "y")
    ]["start_date"].to_list() == [datetime(2019, 12, 1).date()]
    assert result_w_ffil_gaps_true[
        (result_w_ffil_gaps_true["col_1"] == "x") & (result_w_ffil_gaps_true["col_2"] == "y")
    ]["end_date"].to_list() == [datetime(2020, 2, 29).date()]

    assert result_w_ffil_gaps_true[
        (result_w_ffil_gaps_true["col_1"] == "x") & (result_w_ffil_gaps_true["col_2"] == "Y")
    ]["start_date"].to_list() == [datetime(2020, 3, 1).date()]
    assert result_w_ffil_gaps_true[
        (result_w_ffil_gaps_true["col_1"] == "x") & (result_w_ffil_gaps_true["col_2"] == "Y")
    ]["end_date"].to_list() == [datetime(2020, 5, 31).date()]

    assert result_w_ffil_gaps_true[
        (result_w_ffil_gaps_true["col_1"] == "m") & (result_w_ffil_gaps_true["col_2"] == "n")
    ]["start_date"].to_list() == [datetime(2019, 12, 1).date()]
    assert result_w_ffil_gaps_true[
        (result_w_ffil_gaps_true["col_1"] == "m") & (result_w_ffil_gaps_true["col_2"] == "n")
    ]["end_date"].to_list() == [datetime(2020, 2, 29).date()]

    assert sorted(
        result_w_ffil_gaps_true[(result_w_ffil_gaps_true["col_1"] == "a") & (result_w_ffil_gaps_true["col_2"] == "b")][
            "start_date"
        ].to_list()
    ) == [
        datetime(2019, 12, 1).date(),
        datetime(2021, 6, 1).date(),
    ]

    assert sorted(
        result_w_ffil_gaps_true[(result_w_ffil_gaps_true["col_1"] == "a") & (result_w_ffil_gaps_true["col_2"] == "b")][
            "end_date"
        ].to_list()
    ) == [
        datetime(2021, 2, 28).date(),
        datetime(2021, 8, 31).date(),
    ]
