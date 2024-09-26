import logging
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from src.constants import (
    DP4_DESCRIPTOR_NAME,
    DP4_DESCRIPTOR_STATE,
    DP4_DESCRIPTOR_CITY,
    DP4_DESCRIPTOR_TRIMMED_NAME,
    DP4_DESCRIPTOR_TRIMMED_STATE,
    DP4_DESCRIPTOR_TRIMMED_CITY,
    DP4_DESCRIPTOR_COLS,
    RP1_DESCRIPTOR_NAME,
    RP1_DESCRIPTOR_STATE,
    RP1_DESCRIPTOR_CITY,
    RP1_DESCRIPTOR_TRIMMED_NAME,
    RP1_DESCRIPTOR_TRIMMED_STATE,
    RP1_DESCRIPTOR_TRIMMED_CITY,
    RP1_TRIMMED_DESCRIPTOR_COLS,
    RP1_MMHID_COL,
    VALID_FROM_COL,
    VALID_UNTIL_COL,
    JOIN_MECHANISM_EXACT,
    VALID_JOIN_MECHANISMS,
    VALID_JOIN_CONFIGS,
    MAPPED_TXN_DESCRIPTORS_SCHEMA,
    ORDERED_LIST_OF_JOINS,
    N_MMHIDS_MAPPED_THRESHOLD,
    N_MMHIDS_MAPPED_COL,
    JOIN_METHODOLOGY_COL,
    RELEVANT_LOCATIONS_COLS,
    SUMMARIZED_TXNS_DATE_COL,
    SUMMARIZED_TXNS_COLS,
)

from src.join_methodologies import exact_join, startswith_join

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def join_distinct_txn_descriptors_with_rp1(spark, transactions_df, standardized_rp1_descriptors_df):
    """
    Step 1: Joins distinct set of transaction descriptors to RP1 descriptors.

    This step first preprocesses transactions and descriptors, and then
    serially uses all techniques defined in src.constants.ORDERED_LIST_OF_JOINS to
    join distinct DP4 transaction descriptors to RP1 descriptors. IT implements all
    fuzzy joins between the two sets of descriptors, and returns resultant dataframe.

    Parameters
    ----------
        spark: SparkSession
            Active spark session.
        transactions_df: Dataframe
            Spark dataframe of transactions loaded from s3.
        standardized_rp1_descriptors_df: Dataframe
            Spark dataframe of standardized_rp1_descriptors loaded from s3.
    """

    trimmed_rp1_descriptors_df = (
        standardized_rp1_descriptors_df.filter(
            F.col(RP1_DESCRIPTOR_NAME).isNotNull()
        )  # only map descriptors with non-null name
        .withColumn(
            VALID_FROM_COL, F.when(F.col(VALID_FROM_COL).isNull(), F.lit("2000-01-01")).otherwise(F.col(VALID_FROM_COL))
        )
        .withColumn(
            VALID_UNTIL_COL,
            F.when(F.col(VALID_UNTIL_COL).isNull(), F.lit("2100-01-01")).otherwise(F.col(VALID_UNTIL_COL)),
        )
        .withColumn(RP1_DESCRIPTOR_TRIMMED_NAME, F.trim(RP1_DESCRIPTOR_NAME))
        .withColumn(RP1_DESCRIPTOR_TRIMMED_STATE, F.trim(RP1_DESCRIPTOR_STATE))
        .withColumn(RP1_DESCRIPTOR_TRIMMED_CITY, F.trim(RP1_DESCRIPTOR_CITY))
        .select(RP1_TRIMMED_DESCRIPTOR_COLS + [RP1_MMHID_COL, VALID_FROM_COL, VALID_UNTIL_COL])
    )

    txn_descriptors_df = (
        transactions_df.filter(F.col(DP4_DESCRIPTOR_NAME).isNotNull())  # only map descriptors with non-null name
        .select(DP4_DESCRIPTOR_COLS)
        .distinct()
        .withColumn(DP4_DESCRIPTOR_TRIMMED_NAME, F.trim(DP4_DESCRIPTOR_NAME))
        .withColumn(DP4_DESCRIPTOR_TRIMMED_STATE, F.trim(DP4_DESCRIPTOR_STATE))
        .withColumn(DP4_DESCRIPTOR_TRIMMED_CITY, F.trim(DP4_DESCRIPTOR_CITY))
    )

    mapped_txn_descriptors_df = spark.createDataFrame([], MAPPED_TXN_DESCRIPTORS_SCHEMA)
    for mechanism, config in ORDERED_LIST_OF_JOINS:
        if mechanism not in VALID_JOIN_MECHANISMS:
            raise RuntimeError(f"{mechanism} is not a valid join mechanism.")
        if config not in VALID_JOIN_CONFIGS:
            raise RuntimeError(f"{config} is not a valid join configuration.")

        fn = exact_join if mechanism == JOIN_MECHANISM_EXACT else startswith_join

        log.info(f"- rp1_txn_enrich_log: Fuzzy join mechanism {mechanism} with {config} config is now being applied.")
        # Important to remember that fn returns both the dfs as already persisted.
        successfully_mapped_txn_descriptors_df, remaining_txn_descriptors_df = fn(
            txns_df=txn_descriptors_df,
            descriptors_df=trimmed_rp1_descriptors_df,
            **VALID_JOIN_CONFIGS[config],
        )
        mapped_txn_descriptors_df = mapped_txn_descriptors_df.unionByName(successfully_mapped_txn_descriptors_df)

        txn_descriptors_df = remaining_txn_descriptors_df

        remaining_txn_descriptors_df = None

    return mapped_txn_descriptors_df


def join_all_txns_with_rp1(transactions_df, mapped_txn_descriptors_df):
    """
    Step 2: Joins transactions_df with the result of join_distinct_txn_descriptors_with_rp1.

    Since one descriptor could have mapped to multiple RP1_MMHID_COL, this
    step filters transaction-rp1 mapping on the basis of
    - dp4_transaction_date being within rp1_descriptor-RP1_MMHID_COL mapping
      validitity period.
    - ratio of rp1_descriptor_name to dp4_descriptor_name being max and <= 1. This
      is helpful to filter out sub-optimal joins with the startswith mechanism.

    It's important to note that the table returned at the end of this step is
    non-normalized i.e. one transaction maps to multiple RP1_MMHID_COLs in this
    table. This 1-to-many mapping can arise because:

    1. "processed_city" and "without_city" configurations can cause 1 txn-descriptor
       to correspond to multiple rp1-descriptors, if after processing city_name,
       multiple rp1-descriptors (corresponding to different RP1_MMHID_COLs) reduce to
       the same dp4-descriptor.
       e.g.: dp4_merchant_name="ABC", dp4_merchant_state="CA", dp4_merchant_city="+1-234-567-8910"
             rp1_original_name="ABC", imputed_state="CA", imputed_city="+1 (234) 567-8910", rp1_mmhid="666"
             rp1_original_name="ABC", imputed_state="CA", imputed_city="+1 234 567 8910", rp1_mmhid="999"
            In this example, the given dp4 descriptor will be mapped to 2 different mmhids: 666 and 999.

    2. "startswith" mechanism can cause 1 txn-descriptor to correspond to multiple
       rp1-descriptors (all with the same "best" length_ratio), if they correspond to
       different RP1_MMHID_COLs.
       e.g.: dp4_merchant_name="Name_1", dp4_merchant_state="CA", dp4_merchant_city="XYZ"
             rp1_original_name="Name_123", imputed_state="CA", imputed_city="XYZ", rp1_mmhid="666"
             rp1_original_name="Name_1234", imputed_state="CA", imputed_city="XYZ", rp1_mmhid="777"
             rp1_original_name="Name_12345", imputed_state="CA", imputed_city="XYZ", rp1_mmhid="77"
             rp1_original_name="Name_124", imputed_state="CA", imputed_city="XYZ", rp1_mmhid="888"
             rp1_original_name="Name_125", imputed_state="CA", imputed_city="XYZ", rp1_mmhid="999"
            In this example, the given dp4 descriptor will get mapped to 3 different mmhids: 666, 888, 999.
            Note that mmhids 777 and 77 will be filtered out because of "max_length_ratio" computation.

    Parameters
    ----------
        transactions_df: Dataframe
            Spark dataframe of transactions loaded from s3.
        mapped_txn_descriptors_df: Dataframe
            Spark dataframe containing distinct transaction descriptors mapped
            to RP1_MMHID_COL.
    """

    # Window function over mapped_txn_descriptors columns to find
    # rp1_original_name that has the best length ratio with dp4_merchant_name.
    # This can lead to descriptor collision i.e. 1 txn joining with
    # multiple distinct descriptors that startswith the same prefix. In
    # such a case, all candidate descriptors will get mapped to the
    # respective txn, and result in double/over counting of txns.
    # To solve this, group-by the final set of all successfully mapped
    # txns + concatenate distinct values of various metadata columns
    # as a string, separated by some special character (||).
    w = Window.partitionBy(DP4_DESCRIPTOR_COLS)

    return (
        transactions_df.join(
            mapped_txn_descriptors_df,
            on=[DP4_DESCRIPTOR_NAME, DP4_DESCRIPTOR_STATE, DP4_DESCRIPTOR_CITY],
            how="left",
        )
        .filter(
            F.col(RP1_MMHID_COL).isNull()
            | F.col("dp4_transaction_date").between(F.col(VALID_FROM_COL), F.col(VALID_UNTIL_COL))
        )
        .drop(VALID_FROM_COL)
        .drop(VALID_UNTIL_COL)
        .withColumn("length_ratio", F.length(F.trim(DP4_DESCRIPTOR_NAME)) / F.length(RP1_DESCRIPTOR_TRIMMED_NAME))
        .withColumn("max_length_ratio", F.max("length_ratio").over(w))
        .filter(F.col("max_length_ratio").isNull() | (F.col("length_ratio") == F.col("max_length_ratio")))
        .drop("max_length_ratio", "length_ratio")
    )


def normalize_and_enrich_rp1_joined_txns(
    standardized_rp1_locations_df, mapped_txn_descriptors_df, non_normalized_joined_txns_df
):
    """
    Step 3: Enrich joined DP4 transactions with RP1 locations metadata.

    This step joins standardized_rp1_locations metadata with NON_NORMALIZED_JOINED_TXNS_TBL.
    It also takes care of normalizing the set of transactions i.e. ensuring that
    there is only 1 row per transaction in the resultant dataframe. It does so by
    grouping NON_NORMALIZED_JOINED_TXNS_TBL by all non "rp1_" prefixed columns,
    and collecting values of all "rp1_" prefixed columns in sets. Before writing
    the resultant dataframe to s3, all sets are stringified with "||" (double-pipe)
    as the separator.

    Parameters
    ----------
        standardized_rp1_locations_df: Dataframe
            Spark dataframe of standardized_rp1_locations loaded from s3.
        mapped_txn_descriptors_df: Dataframe
            Spark dataframe containing distinct transaction descriptors mapped
            to RP1_MMHID_COL.
        non_normalized_joined_txns_df: DataFrame
            Spark dataframe containing all transactions mapped to RP1_MMHID_COL.
            This dataframe is non-normalized i.e. it may contain multiple rows
            for a single transaction as well.
    """

    # RP1 metadata columns sometimes unexpectedly become NULL. This step ensures
    # that for NULL entries, the last best known value is used instead. This is
    # fine to do because we filter to the latest delivered location metadata for
    # each mmhid anyway in the lines below.
    for col in RELEVANT_LOCATIONS_COLS[1:]:
        standardized_rp1_locations_df = standardized_rp1_locations_df.withColumnRenamed(col, "_" + col).withColumn(
            col, F.last("_" + col, True).over(Window.partitionBy(RP1_MMHID_COL).orderBy("first_seen"))
        )

    locations_df = standardized_rp1_locations_df.filter(F.col("is_latest") == True).select(RELEVANT_LOCATIONS_COLS)

    # Filter location metadata for relevant mmhids only. This helps decrease the
    # footprint of the df and allows it to be broadcastable.
    locations_df = locations_df.join(
        F.broadcast(mapped_txn_descriptors_df.select(RP1_MMHID_COL).distinct()),
        on=RP1_MMHID_COL,
        how="inner",
    )

    non_normalized_joined_txns_wo_rp1_cols = list(
        filter(lambda x: not x.startswith("rp1_"), non_normalized_joined_txns_df.columns)
    )

    all_rp1_cols = locations_df.columns + list(
        filter(lambda x: x.startswith("rp1_"), non_normalized_joined_txns_df.columns)
    )
    all_rp1_cols.remove(RP1_MMHID_COL)  # remove common column that occurs twice

    log.info(f"- rp1_txn_enrich_log: Joining non_normalized_joined_txns_df with locations_df")

    # Fuzzy joins can induce 1-to-many transaction-to-RP1_MMHID_COL mapping because
    # of descriptor collisions (i.e. 1 txn joining with multiple distinct
    # descriptors). This is possible when joins are done while
    # - processing the city name
    # - not including the city name
    # - only considering the startswith condition and not exact match
    # If unhandled, this can lead to overcounting of these transactions.
    # To resolve this, we group-by the set of txn cols and concat
    # distinct values of various metadata columns as a string, separated
    # by some special character. This handles the multiple RP1_MMHID_COL
    # mappings for successfully mapped transactions.
    rp1_enriched_txns = (
        F.broadcast(locations_df)
        .join(non_normalized_joined_txns_df, RP1_MMHID_COL, how="right")
        .repartition(*non_normalized_joined_txns_wo_rp1_cols)
        .groupBy(non_normalized_joined_txns_wo_rp1_cols)  # Account for 1-to-many txn-RP1_MMHID_COL mapping
        .agg(*[F.collect_set(col).alias("_" + col) for col in all_rp1_cols])
        .withColumn(N_MMHIDS_MAPPED_COL, F.size("_" + RP1_MMHID_COL))
    )

    # Upper bound degree of 1-to-many transaction-to-RP1_MMHID_COL mapping.
    # Stringify the collected set of rp1_location columns (including RP1_MMHID_COL).
    for col in all_rp1_cols:
        rp1_enriched_txns = rp1_enriched_txns.withColumn(
            col,
            F.when(
                (F.col(N_MMHIDS_MAPPED_COL) < N_MMHIDS_MAPPED_THRESHOLD) & (F.size("_" + col) > 0),
                F.concat_ws("||", "_" + col),
            ).otherwise(None),
        ).drop("_" + col)

    return rp1_enriched_txns.withColumn(
        N_MMHIDS_MAPPED_COL,
        F.when(F.col(N_MMHIDS_MAPPED_COL) < N_MMHIDS_MAPPED_THRESHOLD, F.col(N_MMHIDS_MAPPED_COL)).otherwise(0),
    ).withColumn(
        JOIN_METHODOLOGY_COL,
        F.when(F.col(N_MMHIDS_MAPPED_COL) < N_MMHIDS_MAPPED_THRESHOLD, F.col(JOIN_METHODOLOGY_COL)).otherwise(None),
    )


def summarize_enriched_txns(rp1_enriched_txns_df, monthly):
    """
    Step 4: Summarize RP1-enriched DP4 transactions.

    Simple groupby of RP1_ENRICHED_TXNS_TBL to a description-grain and
    monthly-description-grain cadence for easy analyses.

    Parameters
    ----------
        rp1_enriched_txns_df: Dataframe
            Spark dataframe loaded from s3 where output of present workflow is stored.
        monthly: boolean
            Whether summarization should be done per-month or over all history.
    """

    groupby_cols = []
    extra_agg_cols = []
    if monthly:
        rp1_enriched_txns_df = rp1_enriched_txns_df.withColumn(
            SUMMARIZED_TXNS_DATE_COL, F.to_date(F.date_trunc("month", F.col("dp4_transaction_date")))
        )
        groupby_cols.append(SUMMARIZED_TXNS_DATE_COL)
    else:
        extra_agg_cols.append(F.min("dp4_transaction_date").alias("first_seen"))
        extra_agg_cols.append(F.max("dp4_transaction_date").alias("last_seen"))

    groupby_cols += SUMMARIZED_TXNS_COLS + RELEVANT_LOCATIONS_COLS

    return rp1_enriched_txns_df.groupBy(groupby_cols).agg(
        F.count(F.expr("*")).alias("n_txns"),
        F.countDistinct("dp4_member_id").alias("n_members"),
        F.sum("dp4_transaction_amt").alias("sum_transaction_amt"),
        F.sum("purchase_amount").alias("sum_purchase_amt"),
        *extra_agg_cols,
    )
