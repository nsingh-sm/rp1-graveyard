from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# All the tables this data-plane writes in artifacts.
NON_NORMALIZED_JOINED_TXNS_TBL = "non_normalized_joined_txns"
RP1_ENRICHED_TXNS_TBL = "rp1_enriched_transactions"
SUMMARIZED_TXNS_TBL = "rp1_transaction_descriptions_summary"
SUMMARIZED_TXNS_MONTHLY_TBL = SUMMARIZED_TXNS_TBL + "_monthly"

# All the hyper-paramters for fuzzy joins.
ARBITRARY_MIN_LENGTH_RATIO = 0.7
N_MMHIDS_MAPPED_THRESHOLD = 10
PREFIX_MATCH_LENGTH = 10

# DP4 descriptor columns.
DP4_DESCRIPTOR_NAME = "dp4_merchant_name"
DP4_DESCRIPTOR_STATE = "dp4_merchant_state"
DP4_DESCRIPTOR_CITY = "dp4_merchant_city_name"
DP4_DESCRIPTOR_COLS = [DP4_DESCRIPTOR_NAME, DP4_DESCRIPTOR_STATE, DP4_DESCRIPTOR_CITY]

DP4_DESCRIPTOR_TRIMMED_NAME = DP4_DESCRIPTOR_NAME + "_trimmed"
DP4_DESCRIPTOR_TRIMMED_STATE = DP4_DESCRIPTOR_STATE + "_trimmed"
DP4_DESCRIPTOR_TRIMMED_CITY = DP4_DESCRIPTOR_CITY + "_trimmed"
DP4_TRIMMED_DESCRIPTOR_COLS = [DP4_DESCRIPTOR_TRIMMED_NAME, DP4_DESCRIPTOR_TRIMMED_STATE, DP4_DESCRIPTOR_TRIMMED_CITY]

# RP1 descriptor columns.
RP1_DESCRIPTOR_NAME = "rp1_original_name"
RP1_DESCRIPTOR_STATE = "imputed_state"
RP1_DESCRIPTOR_CITY = "imputed_city"
RP1_DESCRIPTOR_COLS = [RP1_DESCRIPTOR_NAME, RP1_DESCRIPTOR_STATE, RP1_DESCRIPTOR_CITY]

RP1_DESCRIPTOR_TRIMMED_NAME = RP1_DESCRIPTOR_NAME + "_trimmed"
RP1_DESCRIPTOR_TRIMMED_STATE = RP1_DESCRIPTOR_STATE + "_trimmed"
RP1_DESCRIPTOR_TRIMMED_CITY = RP1_DESCRIPTOR_CITY + "_trimmed"
RP1_TRIMMED_DESCRIPTOR_COLS = [RP1_DESCRIPTOR_TRIMMED_NAME, RP1_DESCRIPTOR_TRIMMED_STATE, RP1_DESCRIPTOR_TRIMMED_CITY]

# Remaining RP1 columns used in joins.
RP1_MMHID_COL = "rp1_mmhid"
VALID_FROM_COL = "valid_from"
VALID_UNTIL_COL = "valid_until"

"""
This data-plane supports join of dp4 and rp1 descriptors using
6 distinct methodologies, made up of 2 mechanisms (exact and startswith)
which can be further configured in 3 different ways.

All methodologies require the state code to match exactly in both sets of
transaction descriptors. The 2 different mechanisms control how 'name' column
is joined on, while the 3 different configurations control how 'city' column
is joined on.

Mechanism-1: exact
- name columns should match exactly between the two sets
  of transaction descriptors.

Mechanism-2: startswith
- name columns only need a prefix/startswith match for
  the join.

Configuration-1: basic
- city columns should match exactly between the two sets
  of transaction descriptors

Configuration-2: processed_city
- city columns are processed to remove all non-alphanumeric characters
  before the join.

Configuration-3: without_city
- city columns need not match for the join.
"""

JOIN_METHODOLOGY_COL = "join_methodology"

JOIN_MECHANISM_EXACT = "exact"
JOIN_MECHANISM_STARTSWITH = "startswith"
VALID_JOIN_MECHANISMS = [JOIN_MECHANISM_EXACT, JOIN_MECHANISM_STARTSWITH]

JOIN_CONFIG_BASIC = "basic"
JOIN_CONFIG_PROCESSED_CITY = "processed_city"
JOIN_CONFIG_WITHOUT_CITY = "without_city"

"""
Implementation wise, the 2 mechanisms diverge significantly, and require
their own distinct functions while the 3 configurations are done through
column manipulation and pre+post process helper functions.

VALID_JOIN_CONFIGS dicts specify the parameters for the 2 functions that
implement the different mechanisms of joins.

ORDERED_LIST_OF_JOINS list specifies the order in which join methodologies
are executed. One transaction descriptor is matched with RP1 using only one
1 methodology i.e. if a transaction descriptor gets mapped using a methodology,
it is not subjected to subsequent methodologies.

MAPPED_TXN_DESCRIPTORS_SCHEMA specifies the schema of the dataframe (returned
by the 2 functions) which contains those transaction descriptors that were
successfully mapped.
"""

VALID_JOIN_CONFIGS = {}
VALID_JOIN_CONFIGS[JOIN_CONFIG_BASIC] = {"configuration_name": "basic", "process_city": False, "include_city": True}

VALID_JOIN_CONFIGS[JOIN_CONFIG_PROCESSED_CITY] = {
    "configuration_name": "processed_city",
    "process_city": True,
    "include_city": True,
}

VALID_JOIN_CONFIGS[JOIN_CONFIG_WITHOUT_CITY] = {
    "configuration_name": "without_city",
    "process_city": False,
    "include_city": False,
}

ORDERED_LIST_OF_JOINS = [
    (JOIN_MECHANISM_EXACT, JOIN_CONFIG_BASIC),
    (JOIN_MECHANISM_EXACT, JOIN_CONFIG_PROCESSED_CITY),
    (JOIN_MECHANISM_STARTSWITH, JOIN_CONFIG_BASIC),
    (JOIN_MECHANISM_STARTSWITH, JOIN_CONFIG_PROCESSED_CITY),
    (JOIN_MECHANISM_EXACT, JOIN_CONFIG_WITHOUT_CITY),
    (JOIN_MECHANISM_STARTSWITH, JOIN_CONFIG_WITHOUT_CITY),
]

MAPPED_TXN_DESCRIPTORS_SCHEMA = StructType(
    [
        StructField(DP4_DESCRIPTOR_NAME, StringType(), False),
        StructField(DP4_DESCRIPTOR_STATE, StringType(), True),
        StructField(DP4_DESCRIPTOR_CITY, StringType(), True),
        StructField(RP1_MMHID_COL, IntegerType(), False),
        StructField(RP1_DESCRIPTOR_TRIMMED_NAME, StringType(), False),
        StructField(VALID_FROM_COL, DateType(), False),
        StructField(VALID_UNTIL_COL, DateType(), False),
        StructField(JOIN_METHODOLOGY_COL, StringType(), False),
    ]
)

# All location metadata columns that transactions are enriched with.
RELEVANT_LOCATIONS_COLS = [
    RP1_MMHID_COL,
    "clean_aggregate_merchant_name",
    "clean_parent_aggregate_merchant_name",
    "rp1_merchant_name_cleansed",
    "rp1_legal_corporate_name_cleansed",
    "rp1_address_cleansed",
    "rp1_postal_code_cleansed",
    "rp1_city_name_cleansed",
    "rp1_state_province_code_cleansed",
    "rp1_phone_number_cleansed",
    "rp1_geocoding_quality_indicator",
    "rp1_latitude",
    "rp1_longitude",
    "rp1_msa_code",
    "rp1_naics_code",
]

# All columns used while generating summary of rp1_enriched_txns table.
SUMMARIZED_TXNS_DATE_COL = "txn_month"
N_MMHIDS_MAPPED_COL = "n_mmhids_mapped"
SUMMARIZED_TXNS_COLS = [
    "dp4_merchant_name",
    "dp4_merchant_state",
    "dp4_merchant_city_name",
    "dp4_merchant_category_xcd",
    "vendor_primary_merchant",
    "vendor_intermediaries",
    "vendor_channel",
    JOIN_METHODOLOGY_COL,
    N_MMHIDS_MAPPED_COL,
    RP1_DESCRIPTOR_TRIMMED_NAME,  # intentionally skip RP1_MMHID_COL, since it's present in RELEVANT_LOCATIONS_COLS
]
