import click
import logging
import os
from pyspark.sql import SparkSession
from src.constants import (
    NON_NORMALIZED_JOINED_TXNS_TBL,
    RP1_ENRICHED_TXNS_TBL,
    SUMMARIZED_TXNS_TBL,
    SUMMARIZED_TXNS_MONTHLY_TBL,
    SUMMARIZED_TXNS_DATE_COL,
)
from src.core import (
    join_distinct_txn_descriptors_with_rp1,
    join_all_txns_with_rp1,
    normalize_and_enrich_rp1_joined_txns,
    summarize_enriched_txns,
)
import pyspark.sql.functions as F


def get_session() -> SparkSession:
    return (
        SparkSession.builder.config("spark.driver.maxResultSize", "16G")
        .config("spark.sql.files.maxPartitionBytes", "2GB")
        .appName("rp1-transaction-enrichment")
        .getOrCreate()
    )


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@click.command()
@click.option("--enriched-transactions", type=str, help="s3 path of dp4 enriched transactions parquet table.")
@click.option("--standardized-rp1-descriptors", type=str, help="s3 path of standardized descriptors parquet table.")
@click.option("--standardized-rp1-locations", type=str, help="s3 path of standardized locations parquet table.")
@click.option(
    "--output-location", type=str, default=os.environ["DATA_PLANE_OUTPUT_PATH"], help="Output location of data"
)
def main(enriched_transactions, standardized_rp1_descriptors, standardized_rp1_locations, output_location):
    """
    This data-plane is divided into 4 steps:

    1. join_distinct_txn_descriptors_with_rp1:
        Joins distinct transaction descriptors (dp4_merchant_name,
        dp4_merchant_state, dp4_merchant_city_name) mapped to 1 or more
        RP1_MMHID_COL (and original_name for debugging purposes).

    2. join_all_txns_with_rp1 --> NON_NORMALIZED_JOINED_TXNS_TBL:
        Contains all transactions, some of which are mapped to 1 or
        more RP1_MMHID_COL (and original_name for debugging purposes).
        Note that this table is non-normalized i.e. there might be
        multiple rows per transaction, and must be dealth with caution.

    3. normalize_and_enrich_rp1_joined_txns --> ENRICHED_TXNS_TBL:
        Contains all transactions with RP1 metadata (where available). Some
        transactions are mapped to multiple RP1_MMHID_COL, and for these, all
        "rp1_" prefixed columns are "||" (double-pipe) separated strings.
        The N_MMHIDS_MAPPED_COL tells the number of RP1_MMHID_COL for which
        metadata columns were combined.

    4. summarize_enriched_txns --> SUMMARIZED_TXNS_TBL, SUMMARIZED_TXNS_MONTHLY_TBL:
        Contains description-grain and monthly-description-grain version
        of RP1_ENRICHED_TXNS_TBL.
    """

    spark = get_session()
    spark.sparkContext.setLogLevel("WARN")

    log.info(f"- rp1_txn_enrich_log: Setting output_location argument as {output_location}")

    ########## Step-1. Join distinct set of transaction descriptors with RP1 descriptors.
    standardized_rp1_descriptors_df = spark.read.parquet(standardized_rp1_descriptors)
    log.info(f"- rp1_txn_enrich_log: Read rp1 standardized descriptors from {standardized_rp1_descriptors}")

    enriched_transactions_df = spark.read.parquet(enriched_transactions)

    mapped_txn_descriptors_df = join_distinct_txn_descriptors_with_rp1(
        spark, enriched_transactions_df, standardized_rp1_descriptors_df
    )

    ########## Step-2. Join enriched_transactions with MAPPED_TXN_DESCRIPTORS_TBL.
    log.info(
        f"- rp1_txn_enrich_log: Join enriched_transactions_df with mapped_txn_descriptors_df to form non_normalized_joined_txns."
    )
    (
        join_all_txns_with_rp1(enriched_transactions_df, mapped_txn_descriptors_df)
        .coalesce(6400)  # We have ~3 TB of data in total.
        .write.parquet(output_location.rstrip("/") + f"/{NON_NORMALIZED_JOINED_TXNS_TBL}")
    )
    log.info(
        f"- rp1_txn_enrich_log: non_normalized_joined_txns written to {output_location}/{NON_NORMALIZED_JOINED_TXNS_TBL}"
    )

    ########## Step-3. Enrich joined DP4 transactions with RP1 locations metadata.
    standardized_rp1_locations_df = spark.read.parquet(standardized_rp1_locations)
    log.info(
        f"- rp1_txn_enrich_log: Read locations' metadata from rp1 standardized locations {standardized_rp1_locations}"
    )

    read_non_normalized_joined_txns_df = spark.read.parquet(
        output_location.rstrip("/") + f"/{NON_NORMALIZED_JOINED_TXNS_TBL}"
    )
    log.info(
        f"- rp1_txn_enrich_log: Read non_normalized_joined_txns from {output_location}/{NON_NORMALIZED_JOINED_TXNS_TBL}."
    )

    (
        normalize_and_enrich_rp1_joined_txns(
            standardized_rp1_locations_df,
            mapped_txn_descriptors_df,
            read_non_normalized_joined_txns_df,
        )
        .withColumn("random_digit", F.substring(F.col("dp4_transaction_id"), 3, 1))
        .repartition("dp4_file_date", "random_digit")
        .drop("random_digit")
        .write.partitionBy("dp4_file_date")
        .parquet(output_location.rstrip("/") + f"/{RP1_ENRICHED_TXNS_TBL}")
    )
    log.info(
        f"- rp1_txn_enrich_log: non_normalized_joined_txns were normalized, enriched with rp1 metadata, and written to {output_location}/{RP1_ENRICHED_TXNS_TBL}"
    )

    ########## Step-4. Summarize RP1-enriched DP4 transactions.
    read_rp1_enriched_txns = spark.read.parquet(output_location.rstrip("/") + f"/{RP1_ENRICHED_TXNS_TBL}")
    log.info(f"- rp1_txn_enrich_log: Read rp1_enriched_txns from {output_location}/{RP1_ENRICHED_TXNS_TBL}.")

    (
        summarize_enriched_txns(read_rp1_enriched_txns, False)
        .coalesce(80)
        .write.parquet(output_location.rstrip("/") + f"/{SUMMARIZED_TXNS_TBL}")
    )
    log.info(f"- rp1_txn_enrich_log: All summarized transactions written to {output_location}/{SUMMARIZED_TXNS_TBL}")

    (
        summarize_enriched_txns(read_rp1_enriched_txns, True)
        .withColumn(
            "random_digit",
            F.when(
                F.col("dp4_merchant_category_xcd").isNotNull(), F.substring(F.col("dp4_merchant_category_xcd"), 2, 1)
            ).otherwise("0"),
        )
        .repartition(SUMMARIZED_TXNS_DATE_COL, "random_digit")
        .drop("random_digit")
        .write.partitionBy(SUMMARIZED_TXNS_DATE_COL)
        .parquet(output_location.rstrip("/") + f"/{SUMMARIZED_TXNS_MONTHLY_TBL}")
    )
    log.info(
        f"- rp1_txn_enrich_log: All monthly summarized transactions written to {output_location}/{SUMMARIZED_TXNS_MONTHLY_TBL}"
    )

    log.info("- rp1_txn_enrich_log: Successfully completed all 4 steps. Stopping spark session now.")
    spark.stop()


if __name__ == "__main__":
    main()
