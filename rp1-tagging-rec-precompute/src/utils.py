from src.constants import (
    FILLNA_TOKEN,
    TRANSITIVITY_OUTPUT_FIELD,
    OUTPUT_FIELD_AFTER_TRANSITIVITY,
)

from pyspark.sql import DataFrame, functions as F


def build_output_field_to_n_txns_df(rp1_transaction_descriptions_summary: DataFrame) -> DataFrame:
    """
    Helper method to calculate total transactions for every value of
    output field (vendor_primary_merchant). This is used to calculate pct gain if
    the transitively suggested output field (vendor_primary_merchant) is accepted as
    the actual value of output_field.

    @param rp1_transaction_descriptions_summary:
        Summarized dataframe of all BSM transactions with mapped RP1 metadata.
    """

    return (
        rp1_transaction_descriptions_summary.filter(F.col(TRANSITIVITY_OUTPUT_FIELD).isNotNull())
        .groupBy(TRANSITIVITY_OUTPUT_FIELD)
        .agg(F.sum("n_txns").alias(f"{OUTPUT_FIELD_AFTER_TRANSITIVITY}_txns"))
        .withColumnRenamed(TRANSITIVITY_OUTPUT_FIELD, OUTPUT_FIELD_AFTER_TRANSITIVITY)
    )


def filter_same_company_collisions(
    all_transitive_relationships_df: DataFrame, brand_id_to_company_ids_map: DataFrame
) -> DataFrame:
    """
    Helper function to weed out collisions for different brands within the same company.
    Since RP1 does not have the same brand distinction as BSM, collisions for different
    brands within the same company are common e.g. SouthwestAirlineBrand and SouthWestInFlight.
    However, these are not meaningful and need not be reviewed. This helper function
    leverages the brand_company_bridge to filter out transitive tagging recommendations
    where the current tagged brand and recommended brand share a company_id.

    @param all_transitive_relationships_df: Dataframe of all transitive recommendations
                                            discovered using RP1.
    @param brand_id_to_company_ids_map: Map from brand_id to list of company_ids
    """
    return (
        all_transitive_relationships_df.join(
            F.broadcast(
                brand_id_to_company_ids_map.withColumnRenamed("brand_id", TRANSITIVITY_OUTPUT_FIELD).withColumnRenamed(
                    "company_ids", TRANSITIVITY_OUTPUT_FIELD + "_companies"
                )
            ),
            on=TRANSITIVITY_OUTPUT_FIELD,
            how="left",
        )
        .join(
            F.broadcast(
                brand_id_to_company_ids_map.withColumnRenamed(
                    "brand_id", OUTPUT_FIELD_AFTER_TRANSITIVITY
                ).withColumnRenamed("company_ids", OUTPUT_FIELD_AFTER_TRANSITIVITY + "_companies")
            ),
            on=OUTPUT_FIELD_AFTER_TRANSITIVITY,
            how="left",
        )
        .filter(
            F.col(TRANSITIVITY_OUTPUT_FIELD + "_companies").isNull()
            | F.col(OUTPUT_FIELD_AFTER_TRANSITIVITY + "_companies").isNull()
            | (
                F.size(
                    F.array_intersect(
                        TRANSITIVITY_OUTPUT_FIELD + "_companies", OUTPUT_FIELD_AFTER_TRANSITIVITY + "_companies"
                    )
                )
                == 0
            )
        )
        .drop(TRANSITIVITY_OUTPUT_FIELD + "_companies")
        .drop(OUTPUT_FIELD_AFTER_TRANSITIVITY + "_companies")
    )


def enrich_transitive_relationships_df(
    transitive_relationships_df: DataFrame, output_field_to_n_txns: DataFrame
) -> DataFrame:
    """
    It's desirable to know the impact of accepting transitive brand recommendations. This function
    calculates that, and prepares the transitive_tagging_recommendations table to be written to disk.

    @param transitive_relationships_df: dataframe of tagging recommendations using transitivity
    @param output_field_to_n_txns: dataframe that maps every vendor_primary_merchant to
                                   total number of transactions associated with it.
    """
    return (
        transitive_relationships_df.join(
            F.broadcast(output_field_to_n_txns), on=OUTPUT_FIELD_AFTER_TRANSITIVITY, how="left"
        )
        .withColumn(  # pct transactions for the suggested output_field (vendor_primary_merchant)
            f"pct_{OUTPUT_FIELD_AFTER_TRANSITIVITY}_txns",
            F.round(100 * F.col("n_txns") / F.col(f"{OUTPUT_FIELD_AFTER_TRANSITIVITY}_txns"), 3),
        )
        .withColumn(  # undo the fillna before writing to s3
            TRANSITIVITY_OUTPUT_FIELD,
            F.when(F.col(TRANSITIVITY_OUTPUT_FIELD) == FILLNA_TOKEN, None).otherwise(F.col(TRANSITIVITY_OUTPUT_FIELD)),
        )
        .drop(f"{OUTPUT_FIELD_AFTER_TRANSITIVITY}_txns")
    )
