import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType


def clean_agg_merch_columns(column_name: str):
    """
    Helper method that helps consolidate NSR and ON-LINE variants of RP1's
    aggregate_merchant_name and parent_aggregate_merchant_name columns into
    vanilla variants. e.g. there could be 3 distinct values of
    rp1_aggregate_merchant_name called ["NIKE", "NIKE (NSR)", "NIKE (ON-LINE)"]
    which essentially mean the same brand.
    This method consolidates those into just 1 rp_aggregate_merchant_name, "NIKE".

    This is done by stripping away ' (NSR)' and ' (ON-LINE)' substrings.
    Furthermore, if a transaction was mapped to multiple stores, and had
    multiple aggregate_merchant_name/parent_aggregate_merchant_names
    (represented in a || separated str fashion), it's possible
    for those to reduce into the same aggregate_merchant_name or
    parent_aggregate_merchant_name value after stripping away NSR and
    ON-LINE substrings. e.g.:

    Before normalization:
    data_row = [dp4_name='abc', dp4_city='xyz', dp4_amt=12 ... rp1_mmhid='12',
                rp1_aggregate_merchant_name='LULU||LULU (ON-LINE)||LULU (NSR)',
                rp1_aggregate_merchant_name='LULU||LULU (ON-LINE)'

    After normalization:
    data_row = [dp4_name='abc', dp4_city='xyz', dp4_amt=12 ... rp1_mmhid='12',
                rp1_aggregate_merchant_name='LULU',
                rp1_aggregate_merchant_name='LULU'

    This helps in better grouping of the same brands together.

    @param column_name: Name of column in a spark dataframe that needs to be cleaned.
    """

    return F.when(F.col(column_name).isNull(), None).otherwise(
        F.concat_ws(
            "||", F.array_distinct(F.split(F.regexp_replace(F.col(column_name), r"\ \((ON-LINE|NSR)\)", ""), r"\|\|"))
        )
    )


def impute_state_and_city_columns(df: DataFrame) -> DataFrame:
    """
    Method to impute state column from original_city when original_state
    is NULL. 30% of RP1 descriptor records have original_state as null. 99%+ of these
    actually have the state code appended at the end of original_city column.
    """

    def impute_state(original_city, original_state):
        """
        This helper function acts as a udf to extract the last two characters from
        original_city column when original_state is NULL.
        """

        # Only apply this technique if original_state is null.
        if original_state is not None:
            return original_state

        if original_city is None:
            return original_state

        city_split = original_city.strip().split(" ")
        if (len(city_split) > 1) and (len(city_split[-1]) == 2):
            return city_split[-1]

        return original_state  # i.e. none

    def impute_city(original_city, original_state, imputed_state):
        """
        When the state code is imputed from original_city column, it also needs to be
        removed from the original_city column, which results in the de facto imputed_city.
        This helper function acts as a udf to erase the last two characters (along with
        remaining trailing spaces) from the original_city column when the state column
        has been imputed from it.
        """

        # only apply this cleanup if state has been imputed.
        if original_state == imputed_state:
            return original_city

        if original_city is None:
            return original_city  # i.e. none

        city_split = original_city.strip().split(" ")
        if (len(city_split) > 1) and (len(city_split[-1]) == 2):
            return " ".join(city_split[:-1]).strip()

        return original_city  # i.e. not none

    impute_state_udf = F.udf(impute_state, StringType())
    impute_city_udf = F.udf(impute_city, StringType())

    return (
        df.withColumn("imputed_state", impute_state_udf(F.col("rp1_original_city"), F.col("rp1_original_state")))
        .withColumn(
            "imputed_city",
            impute_city_udf(F.col("rp1_original_city"), F.col("rp1_original_state"), F.col("imputed_state")),
        )
        .distinct()
    )
