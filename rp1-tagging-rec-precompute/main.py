import click
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

from src.constants import (
    OUTPUT_FIELD_AFTER_TRANSITIVITY,
    TRANSITIVITY_FEATURE_FIELDS_SETS,
    TRANSITIVITY_OUTPUT_FIELD,
    FILLNA_TOKEN,
    TXN_DESRCIPTIONS_SUMMARY_COLS,
)

from src.transitivity_mapper import form_transitive_relationships

from src.utils import (
    build_output_field_to_n_txns_df,
    enrich_transitive_relationships_df,
    filter_same_company_collisions,
)


def get_session() -> SparkSession:
    return SparkSession.builder.appName("rp1-tagging-rec-precompute").getOrCreate()


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@click.command()
@click.option(
    "--rp1-transaction-descriptions-summary",
    type=str,
    required=True,
    help="s3 path of rp1-transaction-descriptions-summary parquet table.",
)
@click.option("--brand-company-bridge", type=str, required=True, help="s3 path of brand company bridge parquet table.")
@click.option(
    "--output-location", type=str, default=os.environ["DATA_PLANE_OUTPUT_PATH"], help="Output location of data"
)
def main(rp1_transaction_descriptions_summary, brand_company_bridge, output_location):
    """
    This data-plane builds a table with pre-computed tagging recommendations from RP1.
    It dumps the following output table containing descriptor-level aggregation of
    BSM transactions:

    rp1_transitive_tagging_recommendations:
        All transactions for which the current brand (incl. NULL) is different from the brand
        recommended by leveraging transitive relationships between vendor_primary_merchant and
        three different sets of feature fields: (rp1_mmhid, dp4_mcc), (rp1_mmhid),
        (clean_aggregate_merchant_name).
    """

    spark = get_session()
    spark.sparkContext.setLogLevel("WARN")

    rp1_txn_descriptions_summary_df = spark.read.parquet(rp1_transaction_descriptions_summary).select(
        TXN_DESRCIPTIONS_SUMMARY_COLS
    )
    log.info(
        f"- rp1_tagging_rec_log: Read rp1 transaction descriptions summary from {rp1_transaction_descriptions_summary}"
    )

    summary_df = (
        rp1_txn_descriptions_summary_df.filter(F.col("rp1_mmhid").isNotNull())  # optimization
        .fillna(FILLNA_TOKEN, subset=TRANSITIVITY_OUTPUT_FIELD)  # fillna to avoid weird null joins and groupbys.
        .withColumn(
            f"pct_{TRANSITIVITY_OUTPUT_FIELD}_txns",
            F.round(100 * F.col("n_txns") / F.sum("n_txns").over(Window.partitionBy(TRANSITIVITY_OUTPUT_FIELD)), 2),
            # Need window function to calculate total number of txns for the output_field.
        )
    )

    # Collect outputs from various transitivity mapping methodologies in this
    # all_transitive_relationships_df. Ensure strict schema checking when merging dfs.
    all_transitive_relationships_df = (
        form_transitive_relationships(summary_df, TRANSITIVITY_FEATURE_FIELDS_SETS[0])
        .withColumn("feature_priority", F.lit(1))
        .cache()
    )

    log.info(
        f"- rp1_tagging_rec_log: transitive_relationships_df with {', '.join(TRANSITIVITY_FEATURE_FIELDS_SETS[0])} features count = {all_transitive_relationships_df.count()}"
    )
    for i, feature_fields in enumerate(TRANSITIVITY_FEATURE_FIELDS_SETS[1:]):
        transitive_relationships_df = (
            form_transitive_relationships(summary_df, feature_fields)
            .withColumn("feature_priority", F.lit(i + 2))
            .cache()
        )
        log.info(
            f"- rp1_tagging_rec_log: transitive_relationships_df with {', '.join(feature_fields)} features count = {transitive_relationships_df.count()}"
        )

        all_transitive_relationships_df = all_transitive_relationships_df.unionByName(
            transitive_relationships_df, allowMissingColumns=False
        )

    all_transitive_relationships_df.cache()
    log.info(
        f"- rp1_tagging_rec_log: After all transitive mapping, all_transitive_relationships_df.count() = {all_transitive_relationships_df.count()}"
    )

    # Filter out duplicate recommendations using different features.
    filtered_transitive_relationships_df = (
        all_transitive_relationships_df.withColumn(
            "best_feature_priority", F.min("feature_priority").over(Window.partitionBy(summary_df.columns))
        )
        .filter(F.col("feature_priority") == F.col("best_feature_priority"))
        .drop("best_feature_priority")
        .drop("feature_priority")
        .cache()
    )
    log.info(
        f"- rp1_tagging_rec_log: After filtering out duplicate recommendations via different features, {filtered_transitive_relationships_df.count()} total transitive recommendations remain"
    )

    # Filter out tagging collisions for brands within the same company.
    brand_id_to_company_ids_map = (
        spark.read.parquet(brand_company_bridge)
        .groupBy("brand_id")
        .agg(F.collect_set("company_id").alias("company_ids"))
    )
    log.info(
        f"- rp1_tagging_rec_log: Summarized brand-company bridge from {brand_company_bridge} to {brand_id_to_company_ids_map.count()} rows"
    )
    filtered_transitive_relationships_df = filter_same_company_collisions(
        filtered_transitive_relationships_df, brand_id_to_company_ids_map
    ).cache()

    log.info(
        f"- rp1_tagging_rec_log: After filtering out collisions within a company, {filtered_transitive_relationships_df.count()} transitive recommendations remain"
    )

    # Map from output_field (vendor_primary_merchant) to total number of txns.
    output_field_to_n_txns = build_output_field_to_n_txns_df(rp1_txn_descriptions_summary_df).cache()
    log.info(f"- rp1_tagging_rec_log: output_field_to_n_txns.count() = {output_field_to_n_txns.count()}")

    (
        enrich_transitive_relationships_df(filtered_transitive_relationships_df, output_field_to_n_txns)
        .repartition(OUTPUT_FIELD_AFTER_TRANSITIVITY)
        .write.partitionBy(OUTPUT_FIELD_AFTER_TRANSITIVITY)
        .parquet(output_location.rstrip("/") + f"/rp1_transitive_tagging_recommendations")
    )

    log.info(f"- rp1_tagging_rec_log: All done, outputs written to {output_location}. Stopping spark session now.")
    spark.stop()


if __name__ == "__main__":
    main()
