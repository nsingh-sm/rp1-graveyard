import logging
from src.constants import (
    PCT_THRESHOLD_FOR_FEATURE_TO_OUTPUT_MAPPING,
    FILLNA_TOKEN,
    TRANSITIVITY_OUTPUT_FIELD,
    OUTPUT_FIELD_AFTER_TRANSITIVITY,
)

from pyspark.sql import DataFrame, functions as F
from pyspark.sql import Window

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def filter_bad_feature_output_relationships(summary_df: DataFrame, feature_fields: list[str]) -> DataFrame:
    """
    Helper method to filter undesirable values of various feature fields,
    if they're present in summary_df. This function acts as a finite
    repository of all filters on various feature fields.

    @param summmary_df: Input dataframe of transactions consolidated at a descriptor-level.
    @param feature_fields: List of columns in summary_df to use as features during transitivity mapping.
    """

    if "clean_aggregate_merchant_name" in feature_fields:
        log.info(
            "- rp1_tagging_rec_log: filter NON-AGGREGATED clean_parent_aggregate_merchant_name and rows mapped to multiple clean_aggregate_merchant_names"
        )
        summary_df = summary_df.filter(F.col("clean_parent_aggregate_merchant_name") != "NON-AGGREGATED").filter(
            ~F.col("clean_aggregate_merchant_name").contains("||")
        )

    if "rp1_mmhid" in feature_fields:
        log.info("- rp1_tagging_rec_log: filter rp1_mmhids with ||")
        summary_df = summary_df.filter(~F.col("rp1_mmhid").contains("||"))

    return summary_df


def get_features_to_output_relationships(summary_df: DataFrame, feature_fields: list[str]) -> DataFrame:
    """
    Helper method that forms relationships between distinct feature_field values
    with distinct transitive_output_field (vendor_primary_merchant) values. In effect,
    summary_df is grouped on feature_fields+output_field columns, and metadata columns are
    computed to quantify strength of a feature_field - output-field relationship.

    @param summmary_df: Input dataframe of transactions consolidated at a descriptor-level.
    @param feature_fields: List of columns in summary_df to use as features during transitivity mapping.
    """

    # assert type(feature_fields) is list
    # assert TRANSITIVITY_OUTPUT_FIELD in summary_df.columns
    # for col in feature_fields:
    #     assert col in summary_df.columns

    log.info(f"- rp1_tagging_rec_log: get_transitive_relationships using {', '.join(feature_fields)} as features")

    # Need window function to calculate total number of input rows
    # for the feature fields.
    feature_fields_window = Window.partitionBy(feature_fields)

    return (
        filter_bad_feature_output_relationships(summary_df, feature_fields)
        .groupBy(*feature_fields, TRANSITIVITY_OUTPUT_FIELD)
        .agg(F.sum("n_txns").alias("n_transitive_txns"))
        .withColumn(
            "transitivity_strength",  # pct_transitive_txns
            F.round(100 * F.col("n_transitive_txns") / F.sum("n_transitive_txns").over(feature_fields_window), 2),
        )
        .drop("n_transitive_txns")
        .filter(
            F.col("transitivity_strength") > PCT_THRESHOLD_FOR_FEATURE_TO_OUTPUT_MAPPING
        )  # filter low degree transitive relationships
        .withColumn(
            "transitivity_distribution",
            F.to_json(
                F.collect_list(F.create_map(TRANSITIVITY_OUTPUT_FIELD, "transitivity_strength")).over(
                    feature_fields_window
                )
            ),
        )
        .filter(
            F.col(TRANSITIVITY_OUTPUT_FIELD) != FILLNA_TOKEN
        )  # transitive relationship with NULL output_field is useless
        .withColumnRenamed(TRANSITIVITY_OUTPUT_FIELD, OUTPUT_FIELD_AFTER_TRANSITIVITY)
        .withColumn("transitive_features", F.lit(", ".join(feature_fields)))
    )


def form_transitive_relationships(summary_df: DataFrame, feature_fields: list[str]) -> DataFrame:
    """
    Control center for transitivity mapping using a given set of features. Transitive mapping is done
    in two steps (0th step comes pre-baked in the data):
    0. Associate input fields with feature_fields:
        - This translates to associating every transaction to a feature_field (rp1_mmhid / dp4_mcc /
          clean_agg_merch_name). This is achieved when BSM transactions are mapped to RP1 merchants.
    1. Associate feature_fields to output field (vendor_primary_merchant):
        - get_features_to_output_relationships method implements this.
    2. Join feature-fields-to-output-field relationships with summary_df
       (a.k.a. input-fields-to-feature-fields relationships) to complete transitive mapping.

    @param summmary_df: Input dataframe of transactions consolidated at a descriptor-level.
    @param feature_fields: List of columns in summary_df to use as features during transitivity mapping.
    """

    features_to_output_relationships_df = get_features_to_output_relationships(summary_df, feature_fields)

    log.info(
        f"- rp1_tagging_rec_log: Found {features_to_output_relationships_df.count()} feature-output relationships using {', '.join(feature_fields)}"
    )

    transitive_relationship_meta_cols = features_to_output_relationships_df.columns
    for feature in feature_fields:
        transitive_relationship_meta_cols.remove(feature)

    # Add "_" prefix to feature column names before joining to prevent column name ambiguity.
    for feature in feature_fields:
        features_to_output_relationships_df = features_to_output_relationships_df.withColumnRenamed(
            feature, "_" + feature
        )

    # Join input dataframe with discovered feature-to-output relationships to complete transitive
    # mapping of input dataframe rows with discovered feature-to-output maps. In effect, the
    # transitive relationship is mapped as input --> feature, feature --> output => input --> output
    return (
        summary_df.join(
            F.broadcast(features_to_output_relationships_df),
            on=[
                summary_df[feature].eqNullSafe(features_to_output_relationships_df["_" + feature])
                for feature in feature_fields
            ],
            how="inner",
        )
        .filter(F.col(TRANSITIVITY_OUTPUT_FIELD) != F.col(OUTPUT_FIELD_AFTER_TRANSITIVITY))
        .select(summary_df.columns + transitive_relationship_meta_cols)
    )
