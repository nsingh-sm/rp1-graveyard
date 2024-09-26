from datetime import datetime
from dateutil.relativedelta import relativedelta
from io import BytesIO
from csv import QUOTE_NONE
import pandas as pd
import logging
from boto3_type_annotations.s3 import Client

from typing import Callable, Dict, Tuple

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DEFAULT_MIN_PERIOD_DATE = datetime(2000, 1, 1).date()
DEFAULT_MAX_PERIOD_DATE = datetime(2099, 12, 31).date()
VALID_TABLE_NAMES = ["descriptors", "locations", "operatinghours"]


def get_all_object_filenames(location: Dict[str, str], s3_reader: Client) -> list:
    """
    Helper method to get all object filenames from s3.
    """
    continuation_token = None
    s3_args = dict(Bucket=location["bucket"], Prefix=location["prefix"], MaxKeys=1000)

    all_object_filenames = []

    while True:
        if continuation_token:
            s3_args["ContinuationToken"] = continuation_token

        try:
            resp = s3_reader.list_objects_v2(**s3_args)
        except Exception as err:
            raise RuntimeError(f"Failed to get object keys.") from err

        contents = resp.get("Contents") or []
        all_object_filenames.extend([obj["Key"].split(location["prefix"])[-1] for obj in contents])

        if not resp.get("IsTruncated"):
            break

        continuation_token = resp.get("NextContinuationToken")

    return all_object_filenames


def _construct_artifacts_output_key(
    filename: str,
    bsm_artifacts_prefix: str,
    period_end_date: datetime.date,
    delivery_date: datetime.date,
) -> str:
    """
    Constructs artifacts key based on RP1's object key.
    Artifacts key is constructed with the data plane output
    name in mind.
    """

    bsm_artifacts_prefix = bsm_artifacts_prefix.rstrip("/")
    for tbl_name in VALID_TABLE_NAMES:
        if tbl_name in filename:
            return f"{bsm_artifacts_prefix}/raw_rp1_{tbl_name}/period_end_date={period_end_date}/delivery_date={delivery_date}/{filename.split('.')[0]}.parquet"

    return f"{bsm_artifacts_prefix}/miscellaneous/{filename}"


def copy_csv_files_to_intake_bucket(
    rp1_location: dict, intake_location: dict, all_rp1_object_filenames: list, s3_client: Client
):
    """
    Copies raw RP1 data (csvs) to the intake bucket. The csvs written to the intake bucket
    are only those that are not already in the intake bucket.

    Parameters
    ----------
        rp1_location: dict
            Dict with bucket and prefix keys specifying location where RP1 delivers
            raw files.
        intake_location: dict
            Dict with bucket and prefix keys specifying location of intake bucket
            where raw files delivered by RP1 are copied and stored.
        all_rp1_object_filenames: list of strs
            Filtered list of s3 keys representing the files delivered by RP1 that
            need to be ingested.
        s3_client: s3 client
            Helps interact with BSM S3.
    """
    all_intake_object_filenames = get_all_object_filenames(intake_location, s3_client)
    log.info("All object filenames at intake_location:" + " , ".join(all_intake_object_filenames) + "\n")

    # List of objects that are not already in the intake bucket
    objects_not_in_intake = [fname for fname in all_rp1_object_filenames if fname not in all_intake_object_filenames]

    # This loop ingests CSVs into the intake bucket.
    # It only ingests files that are not already in the intake bucket.
    # This is our manual implementation of retry resiliency for the intake bucket.
    log.info("Objects to copy to intake location:" + " , ".join(objects_not_in_intake) + "\n")
    for fname in objects_not_in_intake:
        rp1_key = rp1_location["prefix"] + fname
        intake_key = intake_location["prefix"] + fname

        log.info(f"Reading {rp1_key} from {rp1_location['bucket']}")
        try:
            obj = s3_client.get_object(Bucket=rp1_location["bucket"], Key=rp1_key)
            obj_body = obj["Body"].read()
            obj_content_type = obj["ContentType"]
        except Exception as err:
            raise RuntimeError(f"Failed to read RP1 object.") from err

        log.info(f"Writing {intake_key} to {intake_location['bucket']}.")
        try:
            _ = s3_client.put_object(
                Body=obj_body,
                ContentType=obj_content_type,
                Bucket=intake_location["bucket"],
                Key=intake_key,
            )
        except Exception as err:
            raise RuntimeError("Failed to write to intake bucket.") from err

        log.info(f"Successfully ingested to intake: {fname}")


def _form_period_dates_from_delivery_dates(tbl_delivery_dates: list) -> (dict, dict):
    """
    Helper function that forms period_start_dates and period_end_dates
    using delivery_dates. This function returns two dicts, first is the map
    from delivery_date to period_start_date while the second is the map from
    delivery_date to period_end_date.

    Conceptually, a mapping from descriptor to mmhid is considered valid for
    the entire duration between two deliveries.

    NOTE: STRONG ASSUMPTION that a file is never modified after delivery.
    """

    delivery_date_to_period_start_date = {}
    delivery_date_to_period_end_date = {}

    for ix in range(1, len(tbl_delivery_dates) - 1):
        current_delivery_date = tbl_delivery_dates[ix]
        delivery_date_to_period_end_date[current_delivery_date] = current_delivery_date

        previous_delivery_date = tbl_delivery_dates[ix - 1]
        delivery_date_to_period_start_date[current_delivery_date] = previous_delivery_date + relativedelta(days=1)

    # Handle corner cases.
    delivery_date_to_period_start_date[tbl_delivery_dates[0]] = DEFAULT_MIN_PERIOD_DATE
    delivery_date_to_period_end_date[tbl_delivery_dates[0]] = tbl_delivery_dates[0]

    delivery_date_to_period_start_date[tbl_delivery_dates[-1]] = tbl_delivery_dates[-2] + relativedelta(days=1)
    delivery_date_to_period_end_date[tbl_delivery_dates[-1]] = DEFAULT_MAX_PERIOD_DATE

    return delivery_date_to_period_start_date, delivery_date_to_period_end_date


def _prepare_map_of_delivery_dates_to_period_dates(
    rp1_location: dict, all_rp1_object_filenames: list, s3_client: Client
):
    """
    Helper method to create dict of period_start_date and period_end_dates
    with delivery_dates as keys. Basically, loop over all files that will
    be converted to parquet, and enlist all delivery dates. Maintain table
    specific delivery dates and derive period_start_date and period_end_dates
    using these.

    Parameters
    ----------
        rp1_location: dict
            Dict with bucket and prefix keys specifying location where RP1 delivers
            raw files.
        all_rp1_object_filenames: list of strs
            Filtered list of s3 keys representing the files delivered by RP1 that
            need to be ingested.
        s3_client: s3 client
            Helps interact with BSM S3.

    """
    all_delivery_dates = {}
    for tbl_name in VALID_TABLE_NAMES:
        all_delivery_dates[tbl_name] = []

    for fname in all_rp1_object_filenames:
        rp1_key = rp1_location["prefix"] + fname

        try:
            obj = s3_client.get_object(Bucket=rp1_location["bucket"], Key=rp1_key)
            for tbl_name in VALID_TABLE_NAMES:
                if tbl_name in rp1_key:
                    all_delivery_dates[tbl_name].append(obj["LastModified"].date())

        except Exception as err:
            raise RuntimeError(f"Failed to detect delivery date for {rp1_key}.") from err

    for tbl_name in VALID_TABLE_NAMES:
        all_delivery_dates[tbl_name] = sorted(list(set(all_delivery_dates[tbl_name])))

    # Map from delivery dates to period_start_dates and period_end_dates. A mapping from
    # descriptor to mmhid is valid for the entire duration between two deliveries.
    # NOTE: STRONG ASSUMPTION that a file is never modified after delivery.
    delivery_date_to_period_start_date = {}
    delivery_date_to_period_end_date = {}
    for tbl_name in VALID_TABLE_NAMES:
        log.info(f"{tbl_name} delivery_dates: {', '.join([str(dt) for dt in all_delivery_dates[tbl_name]])}")
        (
            delivery_date_to_period_start_date[tbl_name],
            delivery_date_to_period_end_date[tbl_name],
        ) = _form_period_dates_from_delivery_dates(all_delivery_dates[tbl_name])

        for delivery_date in all_delivery_dates[tbl_name]:
            log.info(
                f"{tbl_name} delivery_date={delivery_date}, period_start_date={delivery_date_to_period_start_date[tbl_name][delivery_date]}, period_end_date={delivery_date_to_period_end_date[tbl_name][delivery_date]}"
            )

    return delivery_date_to_period_start_date, delivery_date_to_period_end_date


def form_parquet_and_write_to_artifacts_bucket(
    rp1_location: dict, artifacts_location: dict, all_rp1_object_filenames: list, s3_client: Client
):
    """
    Load sand converts all raw RP1 data csvs to parquet tables and writes to
    artifacts buckets. All RP1 data is re-written as parquet files to the aritfacts
    bucket. This is done to ensure that the data-plane is resilient to platform
    retries. From the artifacts bucket, the data plane's outputs gets exposed to DI.

    Full RP1 object key(s):
    > s3://secondmeasure-rp1/sftp/secondmeasure_rp1_descriptors_{MMMYYYY}.csv
    > s3://secondmeasure-rp1/sftp/secondmeasure_rp1_locations_{MMMYYYY}.csv
    > s3://secondmeasure-rp1/sftp/secondmeasure_rp1_operatinghours_{MMMYYYY}.csv

    Correspodning artifacts key, i.e "data plane output path":
    > s3://bsm-prod-platform-artifacts-use2/data-plane-output/rp1-ingestion/rp1-ingestion-main-{RAND}/raw_rp1_descriptors/period_end_date={derived-from-delivery_dates}/delivery_date={derived-from-last-modification-date-of-object-being-ingested}/secondmeasure_rp1_descriptors_{MMMYYYY}.parquet
    > s3://bsm-prod-platform-artifacts-use2/data-plane-output/rp1-ingestion/rp1-ingestion-main-{RAND}/raw_rp1_locations/period_end_date={derived-from-delivery_dates}/delivery_date={derived-from-last-modification-date-of-object-being-ingested}/secondmeasure_rp1_locations_{MMMYYYY}.parquet
    > s3://bsm-prod-platform-artifacts-use2/data-plane-output/rp1-ingestion/rp1-ingestion-main-{RAND}/raw_rp1_operatinghours/period_end_date={derived-from-delivery_dates}/delivery_date={derived-from-last-modification-date-of-object-being-ingested}/secondmeasure_rp1_operatinghours_{MMMYYYY}.parquet

    Parameters
    ----------
        rp1_location: dict
            Dict with bucket and prefix keys specifying location where RP1 delivers
            raw files.
        artifacts_location: dict
            Dict with bucket and prefix keys specifying location of artifacts bucket
            where parquet tables stored.
        all_rp1_object_filenames: list of strs
            Filtered list of s3 keys representing the files delivered by RP1 that
            need to be ingested.
        s3_client: s3 client
            Helps interact with BSM S3.
    """
    (
        delivery_date_to_period_start_date,
        delivery_date_to_period_end_date,
    ) = _prepare_map_of_delivery_dates_to_period_dates(rp1_location, all_rp1_object_filenames, s3_client)

    # This loop ingests all RP1s files into the output location (i.e. the artifacts
    # bucket). By default, writes to the output location are resilient to retries,
    # which is why we can ingest all files in this loop.
    log.info(f"Ingesting all objects to artifacts location")
    for fname in all_rp1_object_filenames:
        rp1_key = rp1_location["prefix"] + fname

        # Strong assumption that a filename will not have 2 table names.
        for tbl_name in VALID_TABLE_NAMES:
            if tbl_name in fname:
                table_name = tbl_name

        log.info(f"Converting {fname} csv file into pandas dataframe.")
        data_pd = pd.read_csv(
            BytesIO(s3_client.get_object(Bucket=rp1_location["bucket"], Key=rp1_key)["Body"].read()),
            sep="\t",
            dtype="str",
            quoting=QUOTE_NONE,
        )

        # mmhid must be an int
        if "descriptors" in rp1_key or "locations" in rp1_key:
            data_pd = data_pd.astype({"merchant_market_hierarchy_id": int})
        else:
            data_pd = data_pd.astype({"mmhid": int})

        try:
            obj = s3_client.get_object(Bucket=rp1_location["bucket"], Key=rp1_key)
            last_modified_date = obj["LastModified"].date()
        except Exception as err:
            raise RuntimeError(f"Failed to read RP1 object.") from err

        data_pd["period_start_date"] = delivery_date_to_period_start_date[table_name][last_modified_date]

        artifacts_output_key = _construct_artifacts_output_key(
            fname,
            artifacts_location["prefix"],
            delivery_date_to_period_end_date[table_name][last_modified_date],
            last_modified_date,
        )

        output_s3_path = f"s3://{artifacts_location['bucket']}/{artifacts_output_key}"
        log.info(f"Converting pandas df to parquet and writing to {output_s3_path}.")

        try:
            data_pd.to_parquet(output_s3_path)
        except Exception as err:
            raise RuntimeError(f"Failed to write to artifacts bucket.") from err

        log.info(f"Successfully ingested to artifacts: {fname}")
