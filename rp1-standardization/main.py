import click
import os
from pyspark.sql import SparkSession

from src.utils import impute_state_and_city_columns, clean_agg_merch_columns
from src.constants import DESCRIPTOR_TBL, LOCATION_TBL
from src.standardizer import standardize, log


def get_session() -> SparkSession:
    return SparkSession.builder.appName("rp1-standardization").getOrCreate()


@click.command()
@click.option("--raw-rp1-descriptors", type=str, help="s3 path of raw descriptors parquet table.")
@click.option("--raw-rp1-locations", type=str, help="s3 path of raw locations parquet table.")
@click.option(
    "--output-location", type=str, default=os.getenv("DATA_PLANE_OUTPUT_PATH"), help="Output location of data"
)
def main(raw_rp1_descriptors, raw_rp1_locations, output_location):
    spark = get_session()
    spark.sparkContext.setLogLevel("WARN")

    log.info("- rp1_standardize_log : Standardize descriptors")
    (
        impute_state_and_city_columns(standardize(spark, raw_rp1_descriptors, DESCRIPTOR_TBL)).write.parquet(
            output_location.rstrip("/") + "/standardized_rp1_descriptors"
        )
    )

    log.info("- rp1_standardize_log : Standardize locations")
    (
        standardize(spark, raw_rp1_locations, LOCATION_TBL)
        .withColumn("clean_aggregate_merchant_name", clean_agg_merch_columns("rp1_aggregate_merchant_name"))
        .withColumn(
            "clean_parent_aggregate_merchant_name", clean_agg_merch_columns("rp1_parent_aggregate_merchant_name")
        )
        .write.parquet(output_location.rstrip("/") + "/standardized_rp1_locations")
    )

    log.info("- rp1_standardize_log : Stopping spark session.")
    spark.stop()


if __name__ == "__main__":
    main()
