import logging
import pyspark.sql.functions as F

from src.constants import (
    DP4_DESCRIPTOR_COLS,
    DP4_TRIMMED_DESCRIPTOR_COLS,
    DP4_DESCRIPTOR_TRIMMED_NAME,
    DP4_DESCRIPTOR_TRIMMED_STATE,
    DP4_DESCRIPTOR_TRIMMED_CITY,
    RP1_DESCRIPTOR_TRIMMED_NAME,
    RP1_DESCRIPTOR_TRIMMED_STATE,
    RP1_DESCRIPTOR_TRIMMED_CITY,
    RP1_MMHID_COL,
    JOIN_METHODOLOGY_COL,
    JOIN_MECHANISM_EXACT,
    JOIN_MECHANISM_STARTSWITH,
    ARBITRARY_MIN_LENGTH_RATIO,
    PREFIX_MATCH_LENGTH,
    MAPPED_TXN_DESCRIPTORS_SCHEMA,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def _split_mapped_txns_df(_mapped_txns, methodology_name):
    """
    Helper function to split resultant of dataframe join into
    two parts - successful and unsuccessful/remaining.
    """

    successfully_mapped_txns = (
        _mapped_txns.filter(F.col(RP1_MMHID_COL).isNotNull())
        .withColumn(JOIN_METHODOLOGY_COL, F.lit(methodology_name))
        .select(MAPPED_TXN_DESCRIPTORS_SCHEMA.fieldNames())
    )

    remaining_txns = _mapped_txns.filter(F.col(RP1_MMHID_COL).isNull()).select(
        DP4_DESCRIPTOR_COLS + DP4_TRIMMED_DESCRIPTOR_COLS
    )

    return successfully_mapped_txns, remaining_txns


def _preprocess_city(txns_df, descriptors_df, process_city):
    """
    Helper function to remove non-alphanumeric characters from
    columns with city name.
    """

    if not process_city:
        return txns_df, descriptors_df

    def manipulate_city_col(df, city_col_name):
        return df.withColumn("_" + city_col_name, F.col(city_col_name)).withColumn(
            city_col_name, F.regexp_replace(city_col_name, "[^a-zA-Z0-9]", "")
        )

    return manipulate_city_col(txns_df, DP4_DESCRIPTOR_TRIMMED_CITY), manipulate_city_col(
        descriptors_df, RP1_DESCRIPTOR_TRIMMED_CITY
    )


def _postprocess_city(mapped_txns, process_city):
    """
    Helper function that reverts the impact of _preprocess_city by
    restoring the non-alphanumeric characters that it removed.
    """

    if not process_city:
        return mapped_txns

    return mapped_txns.withColumn(DP4_DESCRIPTOR_TRIMMED_CITY, F.col("_" + DP4_DESCRIPTOR_TRIMMED_CITY)).drop(
        "_" + DP4_DESCRIPTOR_TRIMMED_CITY
    )


def exact_join(txns_df, descriptors_df, configuration_name, include_city, process_city):
    """
    This function implements exact join between two sets of transaction descriptors i.e.
    - state column always has to match exactly
    - name column always has to match exactly
    - city column is matched based on configuration of process_city and include_city flags

    This function returns 2 dataframes: successfully_mapped_txns and remaining_txns.
    """

    if process_city is True and include_city is False:
        raise RuntimeError("When process_city flag is True, include_city flag must also be True.")

    _txns_df, _descriptors_df = _preprocess_city(txns_df, descriptors_df, process_city)

    join_conditions = [
        _txns_df[DP4_DESCRIPTOR_TRIMMED_STATE].eqNullSafe(_descriptors_df[RP1_DESCRIPTOR_TRIMMED_STATE]),
        _txns_df[DP4_DESCRIPTOR_TRIMMED_NAME].eqNullSafe(_descriptors_df[RP1_DESCRIPTOR_TRIMMED_NAME]),
    ]

    if include_city:
        join_conditions.append(
            _txns_df[DP4_DESCRIPTOR_TRIMMED_CITY].eqNullSafe(_descriptors_df[RP1_DESCRIPTOR_TRIMMED_CITY]),
        )

    _mapped_txns = _postprocess_city(_txns_df.join(_descriptors_df, join_conditions, how="left"), process_city)

    methodology_name = JOIN_MECHANISM_EXACT + "_" + configuration_name

    successfully_mapped_txns, remaining_txns = _split_mapped_txns_df(_mapped_txns, methodology_name)

    return successfully_mapped_txns, remaining_txns


def startswith_join(txns_df, descriptors_df, configuration_name, include_city, process_city):
    """
    This function implements startswith join between two sets of transaction descriptors i.e.
    - state column always has to match exactly
    - name column has to match for at least PREFIX_MATCH_LENGTH characters and satisfy ARBITRARY_MIN_LENGTH_RATIO
    - city column is matched based on configuration of process_city and include_city flags

    This function returns dataframes: successfully_mapped_txns and remaining_txns.
    """

    if process_city is True and include_city is False:
        raise RuntimeError("When process_city flag is True, include_city flag must also be True.")

    _txns_df, _descriptors_df = _preprocess_city(txns_df, descriptors_df, process_city)

    _txns_df = _txns_df.withColumn("name_prefix", F.substring(DP4_DESCRIPTOR_TRIMMED_NAME, 1, PREFIX_MATCH_LENGTH))
    _descriptors_df = _descriptors_df.withColumn(
        "name_prefix", F.substring(RP1_DESCRIPTOR_TRIMMED_NAME, 1, PREFIX_MATCH_LENGTH)
    )

    join_conditions = [
        _txns_df[DP4_DESCRIPTOR_TRIMMED_STATE].eqNullSafe(_descriptors_df[RP1_DESCRIPTOR_TRIMMED_STATE]),
        _txns_df["name_prefix"].eqNullSafe(_descriptors_df["name_prefix"]),
        ((F.length(DP4_DESCRIPTOR_TRIMMED_NAME) / F.length(RP1_DESCRIPTOR_TRIMMED_NAME)) > ARBITRARY_MIN_LENGTH_RATIO),
        ((F.length(DP4_DESCRIPTOR_TRIMMED_NAME) / F.length(RP1_DESCRIPTOR_TRIMMED_NAME)) < 1),
        (_descriptors_df[RP1_DESCRIPTOR_TRIMMED_NAME].startswith(_txns_df[DP4_DESCRIPTOR_TRIMMED_NAME])),
    ]

    if include_city:
        join_conditions.append(
            _txns_df[DP4_DESCRIPTOR_TRIMMED_CITY].eqNullSafe(_descriptors_df[RP1_DESCRIPTOR_TRIMMED_CITY]),
        )

    _mapped_txns = _postprocess_city(_txns_df.join(_descriptors_df, join_conditions, how="left"), process_city)

    methodology_name = JOIN_MECHANISM_STARTSWITH + "_" + configuration_name

    successfully_mapped_txns, remaining_txns = _split_mapped_txns_df(_mapped_txns, methodology_name)

    return successfully_mapped_txns, remaining_txns
