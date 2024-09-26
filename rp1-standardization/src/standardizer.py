import logging
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from src.compression_tools import create_hash_columns, compress_table
from src.constants import COLS_DICT, DESCRIPTOR_TBL, LOCATION_TBL

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def _filter_data_for_latest_delivery_date(df: DataFrame, start_date_col, end_date_col) -> DataFrame:
    """
    In case of restatements, data for the same period might be delivered multiple times.
    While standardizing, the latest delivered data should be used.
    """

    # Find the latest delivery date for every period.
    latest_delivery_df = df.groupBy([start_date_col, end_date_col]).agg(
        F.max("delivery_date").alias("latest_delivery_date")
    )

    return (
        df.join(latest_delivery_df, on=[start_date_col, end_date_col], how="left")
        .filter(F.col("delivery_date") == F.col("latest_delivery_date"))
        .drop("latest_delivery_date")
    )


def _filter_data_where_all_columns_are_null(df: DataFrame, cols_list: list) -> DataFrame:
    """
    Remove rows where all columns in cols_list are null.
    """
    return df.filter(sum([F.col(col).isNull().cast("int") for col in cols_list]) != len(cols_list))


def _load_and_filter_dfs(spark: SparkSession, s3_path: str, cols_dict: dict) -> DataFrame:
    """
    Read ingested parquet tables at the given s3 path.
    The tables will be filtered for the latest delivery for
    every end_date, also removing any records where the primary
    keys map to 2 distinct values in the same period.

    Parameters
    ----------
      spark: SparkSession
          Active spark session.
      s3_path: str
          The s3 path of the ingested parquet table that needs to be
          standardized.
      cols_dict: dict
          Either one of constants.COLS_DICT[DESCRIPTOR_TBL] or
          constants.COLS_DICT[LOCATION_TBL]

    Returns
    -------
      Spark Dataframe
          Ingested spark dataframe, filtered for latest deliveries
          per period, and without any keys that correspond to the
          multiple values within the same period.
    """

    log.info(f"- rp1_standardize_log : Reading parquet table from {s3_path}.")

    df = spark.read.parquet(s3_path)

    # filter out rows where all keys or all values are null.
    df = _filter_data_where_all_columns_are_null(
        _filter_data_where_all_columns_are_null(
            (
                df.withColumn("mmhid", F.col("merchant_market_hierarchy_id").cast("int")).drop(
                    "merchant_market_hierarchy_id"
                )
            ),
            cols_dict["primary_keys"],
        ),
        cols_dict["data_cols"],
    )
    log.info(f"- rp1_standardize_log : Parquet table at {s3_path} has {df.count()} rows.")

    # For every period, only retain the latest delivery's data.
    latest_df = (
        _filter_data_for_latest_delivery_date(df, cols_dict["start"], cols_dict["end"]).drop("delivery_date").cache()
    )

    log.info(f"- rp1_standardize_log : After selecting latest delivery data, {latest_df.count()} rows remain.")

    # Remove keys that correspond to multiple values in the same period.
    # Here, we forcefully set data_cols' values to "$_null" since pyspark
    # countDistinct is not nullsafe, and ignores all rows with null values.
    values_per_key_per_period_df = (
        latest_df.fillna("$_null", subset=cols_dict["data_cols"])
        .groupBy(cols_dict["primary_keys"] + [cols_dict["start"], cols_dict["end"]])
        .agg(F.countDistinct(*cols_dict["data_cols"]).alias("count"))
        .select(
            ["count"]
            + [
                F.col(colname).alias("_" + colname)
                for colname in cols_dict["primary_keys"] + [cols_dict["start"], cols_dict["end"]]
            ]
        )
    )

    return (
        latest_df.join(
            values_per_key_per_period_df,
            on=[
                latest_df[col].eqNullSafe(values_per_key_per_period_df["_" + col])
                for col in cols_dict["primary_keys"] + [cols_dict["start"], cols_dict["end"]]
            ],
            how="left",
        )
        .filter(F.col("count") == 1)
        .select([latest_df[col] for col in latest_df.columns])
    )


def _add_prefix(cols: list) -> list:
    return ["rp1_" + col for col in cols]


def _add_prefix_as_spark_cols(cols: list) -> list:
    return [F.col(col).alias("rp1_" + col) for col in cols]


def _assign_validity_periods(standardized_df: DataFrame, cols_dict: dict) -> DataFrame:
    """
    Helper function to add validity period columns to standardized dataframe.
    Essentially, this function adds the prefix 'rp1_' to columns provided by
    RP1, adds the "valid_from" column and updates the "valid_until" column.

    Parameters
    ----------
      standardized_df: Standardized dataframe
          Spark dataframe after standardization.
      cols_dict: dict
          Either one of constants.COLS_DICT[DESCRIPTOR_TBL] or
          constants.COLS_DICT[LOCATION_TBL]

    Returns
    -------
      DataFrame
          Standardized dataframe with the new "valid_from"
          and updated "valud_until" columns.
    """

    w_first_seen = Window.partitionBy(cols_dict["primary_keys"]).orderBy(F.col("first_seen"))

    w_valid_until = Window.partitionBy(cols_dict["primary_keys"]).orderBy(F.col("valid_until").desc())

    return (
        standardized_df.withColumn("row_first_seen", F.row_number().over(w_first_seen))
        .withColumn("valid_from", F.when(F.col("row_first_seen") == 1, None).otherwise(F.col("first_seen")))
        .withColumn("row_valid_until", F.row_number().over(w_valid_until))
        .withColumn("valid_until_new", F.when(F.col("row_valid_until") == 1, None).otherwise(F.col("valid_until")))
        .drop("row_first_seen")
        .drop("row_valid_until")
        .drop("valid_until")
        .withColumnRenamed("valid_until_new", "valid_until")
        .withColumn("is_latest", F.when(F.col("valid_until").isNull(), True).otherwise(False))
        .select(
            _add_prefix_as_spark_cols(list(filter(lambda x: "imputed" not in x, cols_dict["primary_keys"])))
            + list(filter(lambda x: "imputed" in x, cols_dict["primary_keys"]))
            + _add_prefix_as_spark_cols(cols_dict["data_cols"])
            + ["valid_from", "first_seen", "last_seen", "valid_until", "is_latest"]
        )
        .orderBy(["first_seen"] + _add_prefix(list(filter(lambda x: "imputed" not in x, cols_dict["primary_keys"]))))
    )


def standardize(spark: SparkSession, s3_path: str, tbl: str) -> DataFrame:
    """
    Actual function that standardizes descriptors and locations ingested
    after RP1 data deliveries.

    Parameters
    ----------
      spark: SparkSession
          Active spark session.
      s3_path: str
          The s3 path of the ingested parquet table that needs to be
          standardized.
      prefix: str
          The prefix inside bucket where RP1 ingested parquet tables
          live. This function expects files with name "*_MmmYYYY.parquet"
          to exist at the specified s3://{bucket}/{prefix}.
      tbl: str
          Either of constants.DESCRIPTOR_TBL or constants.LOCATION_TBL

    Returns
    -------
      DataFrame
          Standardized table
    """

    assert tbl == DESCRIPTOR_TBL or tbl == LOCATION_TBL

    log.info("- rp1_standardize_log : Load and filter ingested parquet table.")
    filtered_ingest_df = _load_and_filter_dfs(spark, s3_path, COLS_DICT[tbl])
    log.info(f"- rp1_standardize_log : Filtered ingested df has {filtered_ingest_df.count()} rows.")

    log.info(f"- rp1_standardize_log : Hash the filtered-ingested {tbl} dfs")
    hashed_df = create_hash_columns(filtered_ingest_df, COLS_DICT[tbl]["primary_keys"], COLS_DICT[tbl]["data_cols"])

    # hashed_df is used multiple times.
    hashed_df.cache()

    log.info(f"- rp1_standardize_log : Fill compress the hashed {tbl} df")

    # end_date with ffill_gaps=False gives period_end_date (from rp1-ingestion-data-plane)
    # on which the primary_keys was/were seen corresponding to the data_cols for the last time.
    # This is the day of the first delivery in which we observe this correspondance to change
    # (which in turn creates a new record in this table).
    fill_compressed_df = compress_table(
        hashed_df,
        COLS_DICT[tbl]["primary_keys"],
        "data_key_hash",
        COLS_DICT[tbl]["start"],
        COLS_DICT[tbl]["end"],
        fill_gaps=True,
        ffil_gaps=False,
    ).select(
        COLS_DICT[tbl]["primary_keys"]
        + COLS_DICT[tbl]["data_cols"]
        + [F.col("start_date").alias("first_seen"), F.col("end_date").alias("last_seen")]
    )

    # end_date with ffill_gaps=True gives date=1 day before the
    # start_date on which data_cols for a set of primary keys changes.
    # Therefore, this is the date until which we may consider
    # a set of primary keys to correspond to a set of data_cols.
    log.info(f"- rp1_standardize_log : Ffil compress the hashed {tbl} df")
    ffil_compressed_df = compress_table(
        hashed_df,
        COLS_DICT[tbl]["primary_keys"],
        "data_key_hash",
        COLS_DICT[tbl]["start"],
        COLS_DICT[tbl]["end"],
        fill_gaps=False,
        ffil_gaps=True,
    ).select(
        COLS_DICT[tbl]["primary_keys"]
        + COLS_DICT[tbl]["data_cols"]
        + [F.col("start_date").alias("first_seen"), F.col("end_date").alias("valid_until")]
    )

    if fill_compressed_df.count() != ffil_compressed_df.count():
        raise RuntimeError(
            "Outputs of compression tools with ffil=True differ in lengths. Check sort-order of date columns in the input df of compression tools."
        )

    log.info("- rp1_standardize_log : Join Fill and Ffil compressed dfs")
    standardized_df = (
        fill_compressed_df.join(
            ffil_compressed_df,
            [
                fill_compressed_df[col].eqNullSafe(ffil_compressed_df[col])
                for col in (COLS_DICT[tbl]["primary_keys"] + ["first_seen"])
            ],
            "inner",
        )
        .select(
            [fill_compressed_df[col] for col in COLS_DICT[tbl]["primary_keys"]]
            + [fill_compressed_df[col] for col in COLS_DICT[tbl]["data_cols"]]
            + [fill_compressed_df["first_seen"], "last_seen", "valid_until"]
        )
        .cache()
    )

    log.info(f"- rp1_standardize_log : standardized_df.count() = {standardized_df.count()}")
    log.info(
        f"- rp1_standardize_log : standardized_df distinct primary_keys+first_seen count = {standardized_df.select(COLS_DICT[tbl]['primary_keys'] + ['first_seen']).distinct().count()}"
    )
    log.info(
        f"- rp1_standardize_log : standardized_df distinct primary_keys+valid_until count = {standardized_df.select(COLS_DICT[tbl]['primary_keys'] + ['valid_until']).distinct().count()}"
    )

    if standardized_df.count() != fill_compressed_df.count():
        raise RuntimeError(
            "Compression tools report different number of rows in input and output. Check primary keys of input df to compression tools."
        )
    if standardized_df.filter(standardized_df.last_seen.isNull()).count() > 0:
        raise RuntimeError(
            "Compression tools resultant dataframe has rows with null last_seen column. This is not expected."
        )
    if standardized_df.filter(standardized_df.valid_until.isNull()).count() > 0:
        raise RuntimeError(
            "Compression tools resultant dataframe has rows with null valid_until column. This is not expected."
        )
    if (
        standardized_df.count()
        != standardized_df.select(COLS_DICT[tbl]["primary_keys"] + ["first_seen"]).distinct().count()
    ):
        raise RuntimeError(
            "Compression tools resultant dataframe violates primary_key check. Specifically, we observe duplicate first_seen values for the same set of descriptors/mmhid."
        )
    if (
        standardized_df.count()
        != standardized_df.select(COLS_DICT[tbl]["primary_keys"] + ["valid_until"]).distinct().count()
    ):
        raise RuntimeError(
            "Compression tools resultant dataframe violates primary_key check. Specifically, we observe duplicate valid_until values for the same set of descriptors/mmhid."
        )

    log.info("- rp1_standardize_log : Assign validity columns to joined df")
    return _assign_validity_periods(standardized_df, COLS_DICT[tbl])
