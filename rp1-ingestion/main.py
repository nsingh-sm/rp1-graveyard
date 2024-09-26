import boto3
import click
import logging
from src.rp1_ingestion import (
    get_all_object_filenames,
    copy_csv_files_to_intake_bucket,
    form_parquet_and_write_to_artifacts_bucket,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@click.command()
@click.option(
    "--rp1-bucket",
    type=str,
    help="S3 bucket where RP1 delivers files.",
)
@click.option(
    "--rp1-prefix",
    type=str,
    help="S3 prefix (excludes bucket and filename) of RP1's delivered files.",
)
@click.option(
    "--bsm-intake-bucket",
    type=str,
    help="BSM's intake bucket for RP1 data.",
)
@click.option(
    "--bsm-intake-prefix",
    type=str,
    help="S3 prefix (excludes bucket and filename) for intake bucket where RP1 data is copied.",
)
@click.option("--output-location", type=str, help="Output location of data.")
def main(rp1_bucket, rp1_prefix, bsm_intake_bucket, bsm_intake_prefix, output_location):
    log.info("Creating S3 client.")
    try:
        s3_client = boto3.client("s3")
    except Exception as err:
        raise RuntimeError("Failed to create s3-client:") from err

    log.info("Constructing artifacts paths.")
    # Create the bucket and prefix of the platform provisioned
    # output_location. Sample output_location:
    # s3://bsm-prod-platform-artifacts-{region}/data-plane-output/{data-plane-name}/{data-plane-name}-{RAND}/
    artifacts_bucket = output_location.split("s3://")[1].split("/")[0]
    artifacts_prefix = "/".join(output_location.split("s3://")[1].split("/")[1:])

    log.info(f"RP1 bucket: {rp1_bucket}, prefix: {rp1_prefix}")
    rp1_location = {"bucket": rp1_bucket, "prefix": rp1_prefix}

    log.info(f"BSM Intake bucket: {bsm_intake_bucket}, prefix: {bsm_intake_prefix}")
    intake_location = {"bucket": bsm_intake_bucket, "prefix": bsm_intake_prefix}

    log.info(f"Artifacts bucket: {artifacts_bucket}, prefix: {artifacts_prefix}")
    artifacts_location = {
        "bucket": artifacts_bucket,
        "prefix": artifacts_prefix,
    }

    log.info("Now ingesting RP1 data.")

    # Only ingest files with "secondmeasure_places" in filename.
    # Exclude March-2020 descriptors delivery since it was malformed,
    # and re-delivered in April-2020. Note that locations and
    # operatinghours tables were not redelivered.
    log.info("Getting keys of all objects to ingest.")
    all_rp1_object_filenames = list(
        filter(
            lambda filename: ("secondmeasure_places" in filename)
            and filename.endswith(".csv")
            and ("descriptors_Mar2020" not in filename),
            get_all_object_filenames(rp1_location, s3_client),
        )
    )
    log.info("All eligible object filenames at rp1_location:" + " , ".join(all_rp1_object_filenames))

    copy_csv_files_to_intake_bucket(rp1_location, intake_location, all_rp1_object_filenames, s3_client)

    form_parquet_and_write_to_artifacts_bucket(rp1_location, artifacts_location, all_rp1_object_filenames, s3_client)


if __name__ == "__main__":
    main()
