import pytest
from datetime import datetime
from dateutil.relativedelta import relativedelta

from src.rp1_ingestion import (
    _construct_artifacts_output_key,
    _form_period_dates_from_delivery_dates,
    DEFAULT_MIN_PERIOD_DATE,
    DEFAULT_MAX_PERIOD_DATE,
)


def test_construct_artifacts_key():
    bsm_artifacts_prefix = "s3://bsm-prod-platform-artifacts-use2/data-plane-output/rp1-ingestion"
    filename = "secondmeasure_rp1_descriptors_Jan2023.csv"
    delivery_date = datetime.strptime(filename.split(".")[0].split("_")[-1], "%b%Y").date()
    period_end_date = delivery_date - relativedelta(days=1)
    ret = _construct_artifacts_output_key(filename, bsm_artifacts_prefix, period_end_date, delivery_date)
    assert (
        ret
        == f"{bsm_artifacts_prefix}/raw_rp1_descriptors/period_end_date={period_end_date}/delivery_date={delivery_date}/{filename.split('.')[0]}.parquet"
    )

    filename = "secondmeasure_rp1_locations_Jan2023.csv"
    delivery_date = datetime.strptime(filename.split(".")[0].split("_")[-1], "%b%Y").date()
    period_end_date = delivery_date - relativedelta(days=1)
    ret = _construct_artifacts_output_key(filename, bsm_artifacts_prefix, period_end_date, delivery_date)
    assert (
        ret
        == f"{bsm_artifacts_prefix}/raw_rp1_locations/period_end_date={period_end_date}/delivery_date={delivery_date}/{filename.split('.')[0]}.parquet"
    )


def test_form_period_dates_from_delivery_dates():
    delivery_dates = [
        datetime(2020, 3, 20).date(),
        datetime(2020, 6, 18).date(),
        datetime(2020, 9, 25).date(),
        datetime(2021, 3, 9).date(),
    ]

    delivery_date_to_period_start_date, delivery_date_to_period_end_date = _form_period_dates_from_delivery_dates(
        delivery_dates
    )

    assert sorted(list(delivery_date_to_period_start_date.keys())) == sorted(delivery_dates)
    assert sorted(list(delivery_date_to_period_end_date.keys())) == sorted(delivery_dates)

    assert delivery_date_to_period_start_date[datetime(2020, 3, 20).date()] == DEFAULT_MIN_PERIOD_DATE
    assert delivery_date_to_period_end_date[datetime(2020, 3, 20).date()] == datetime(2020, 3, 20).date()

    assert delivery_date_to_period_start_date[datetime(2020, 6, 18).date()] == datetime(2020, 3, 21).date()
    assert delivery_date_to_period_end_date[datetime(2020, 6, 18).date()] == datetime(2020, 6, 18).date()

    assert delivery_date_to_period_start_date[datetime(2020, 9, 25).date()] == datetime(2020, 6, 19).date()
    assert delivery_date_to_period_end_date[datetime(2020, 9, 25).date()] == datetime(2020, 9, 25).date()

    assert delivery_date_to_period_start_date[datetime(2021, 3, 9).date()] == datetime(2020, 9, 26).date()
    assert delivery_date_to_period_end_date[datetime(2021, 3, 9).date()] == DEFAULT_MAX_PERIOD_DATE
