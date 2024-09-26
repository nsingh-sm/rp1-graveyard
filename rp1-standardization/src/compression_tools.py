import pyspark.sql.functions as sqlf
from pyspark.sql import Window
from pyspark.sql.types import StringType


def create_hash_columns(df, primary_keys=[], data_columns=[], characters=256):
    """
    Uses the column names in `primary_keys` list
    and `data_columns` list to create `primary_key_hash`
    and `data_key_hash` respectively
    Args:
        df: spark dataframe
        primary_keys: list of column names which make the
        primary keys of the `df`
        data_columns: list of column names which contain the data associated
        each primary key of the row
    """
    primary_key_strings = [c + "_string" for c in primary_keys]
    data_key_strings = [c + "_string" for c in data_columns]

    df = df.select(
        df.columns + [sqlf.col(c).cast(StringType()).alias(c + "_string") for c in primary_keys + data_columns]
    )

    if primary_keys:
        df = df.withColumn(
            "primary_key_hash",
            sqlf.sha2(sqlf.concat_ws("||", *primary_key_strings), characters),
        )
    if data_columns:
        df = df.withColumn(
            "data_key_hash",
            sqlf.sha2(sqlf.concat_ws("||", *data_key_strings), characters),
        )

    return df.drop(*primary_key_strings).drop(*data_key_strings)


def compress_table(
    odf,
    partition_cols,
    data_col,
    start_date_col="start_date",
    end_date_col="end_date",
    fill_gaps=False,
    ffil_gaps=False,
):
    """
    start_date_col and end_date_col can point to the same column
    fill_gaps = fill empty gaps between the same revisions
    ffil_gaps = forward fill gaps between different revisions
    ffil_gaps intrinsically causes fill_gaps=True
    """

    # If the same column is specified for start/end, we need to create new star/end date cols
    if start_date_col == end_date_col:
        df = odf.withColumn("start_date", sqlf.col(start_date_col)).withColumn("end_date", sqlf.col(end_date_col))
        df = df.drop(start_date_col)
        start_date_col = "start_date"
        end_date_col = "end_date"
    else:
        df = odf.withColumn("start_date", sqlf.col(start_date_col)).withColumn("end_date", sqlf.col(end_date_col))

    df = overlap_delta(
        df,
        partition_cols=partition_cols,
        start_col=start_date_col,
        end_col=end_date_col,
    )

    # window for identifying revision/republishing dupes
    window2 = Window.orderBy(start_date_col).partitionBy(*partition_cols)
    window3 = Window.orderBy(sqlf.col(start_date_col).desc()).partitionBy(*partition_cols)

    window4 = (
        Window.orderBy(start_date_col)
        .partitionBy(*partition_cols)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    if fill_gaps is False:
        get_first = sqlf.when(
            (sqlf.lag(data_col, 1).over(window2) == sqlf.col(data_col)) & (sqlf.col("gap_past") == 0),
            False,
        ).otherwise(True)

        get_last = sqlf.when(
            (sqlf.lag(data_col, 1).over(window3) == sqlf.col(data_col)) & ((sqlf.col("gap_future") == 0)),
            False,
        ).otherwise(True)
    else:
        get_first = sqlf.when((sqlf.lag(data_col, 1).over(window2) == sqlf.col(data_col)), False).otherwise(True)

        get_last = sqlf.when((sqlf.lag(data_col, 1).over(window3) == sqlf.col(data_col)), False).otherwise(True)

    get_serial_no = (
        sqlf.when(sqlf.col("first") & sqlf.col("last"), sqlf.col(start_date_col))
        .when(sqlf.col("first"), sqlf.col(start_date_col))
        .when(sqlf.col("last"), sqlf.lag(start_date_col, 1).over(window2))
    )

    get_row_numbers = sqlf.sum("rows_compressed").over(window4)  # create a rolling sumn of 'rows_compressed'
    get_count_compressed_rows = sqlf.col("row_number") - sqlf.lag("row_number", 1, 0).over(
        window2
    )  # adds compressed rows together
    get_day_range = (
        sqlf.datediff(sqlf.col(end_date_col), sqlf.col("start_date")) + 1
    )  # computes interval between end_date and start_date

    if "rows_compressed" not in df.columns:
        df = df.withColumn("rows_compressed", sqlf.lit(1))
    #         df = df.withColumn('rows_compressed', sqlf.row_number().over(window2))

    sdf2 = (
        df.withColumn("last", get_last)
        .withColumn("first", get_first)
        .withColumn("row_number", get_row_numbers)
        .filter(sqlf.col("first") | sqlf.col("last"))
        .withColumn("start_date_new", get_serial_no)
    )
    cols = df.columns
    cols.remove("gap_future")
    cols.remove("gap_past")  # start_date_col)

    sdf3 = (
        sdf2.drop("start_date")
        .where(sqlf.col("last"))
        .withColumnRenamed("start_date_new", "start_date")
        .withColumn("rows_compressed", get_count_compressed_rows)
        .drop("first", "last")
        .select(*cols)
        .withColumn("day_range", get_day_range)
    )
    sdf4 = overlap_delta(
        sdf3,
        partition_cols=partition_cols,
        start_col=start_date_col,
        end_col=end_date_col,
    )
    if ffil_gaps is True:
        get_filled_date = sqlf.when(
            (sqlf.col("gap_future") == 0) | (sqlf.col("gap_future").isNull()),
            sqlf.col("end_date"),
        ).otherwise(sqlf.date_sub(sqlf.lead("start_date", 1).over(window2), 1))
        sdf4 = compress_table(
            sdf4.withColumn("end_date", get_filled_date),
            partition_cols,
            data_col,
            fill_gaps=True,
        )
    sdf4 = sdf4.withColumn("comp_frac", sqlf.col("rows_compressed") / sqlf.col("day_range"))
    #     print('df:')
    #     df.show(10,80)
    #     print('sdf2:')
    #     sdf2.show(20,80)
    #     print('sdf3:')
    #     sdf3.show(20,80)
    #     print('sdf4:')
    #     sdf4.show(250,80)
    return sdf4


def overlap_delta(df, partition_cols, start_col="start_date", end_col="end_date", extended=False):
    """
    This function computes the gaps between range records.
    """
    w = Window.partitionBy(*partition_cols).orderBy(start_col)
    w2 = Window.partitionBy(*partition_cols).orderBy(sqlf.desc(start_col))
    if extended:
        df2 = df.withColumn(
            "gap_past",
            sqlf.datediff(sqlf.col(start_col), sqlf.lag(end_col, 1).over(w)) - 1,
        )
        df2 = df2.withColumn(
            "gap_future",
            sqlf.datediff(sqlf.lead(start_col, 1).over(w), sqlf.col(end_col)) - 1,
        )
        df2 = df2.withColumn("prior_end_plus", sqlf.lag(end_col, 1).over(w) + 1)
        df2 = df2.withColumn("future_start_minus", sqlf.lead(start_col, 1).over(w) - 1)
    else:
        df2 = df.withColumn(
            "gap_past",
            sqlf.datediff(sqlf.col(start_col), sqlf.lag(end_col, 1).over(w)) - 1,
        )
        df2 = df2.withColumn(
            "gap_future",
            sqlf.datediff(sqlf.lead(start_col, 1).over(w), sqlf.col(end_col)) - 1,
        )

    return df2
