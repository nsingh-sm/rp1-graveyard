# A particular value of feature_fields (e.g. rp1_mmhid) can correspond to multiple
# values of output_field (vendor_primary_merchant). This relationship is only
# valuable if it occurs for a substantial proportion of times for the feature value.
# This constant enforces a lower threshold on the pct of transactions for the feature
# value in which it co-occurs with an output_field value.
PCT_THRESHOLD_FOR_FEATURE_TO_OUTPUT_MAPPING = 5

# Used to impute NULL values in TRANSITIVITY_OUTPUT_FIELD with a placeholder string.
FILLNA_TOKEN = "NULL"

TRANSITIVITY_OUTPUT_FIELD = "vendor_primary_merchant"
OUTPUT_FIELD_AFTER_TRANSITIVITY = "transitive_" + TRANSITIVITY_OUTPUT_FIELD

TRANSITIVITY_FEATURE_FIELDS_SETS = [
    ["rp1_mmhid", "dp4_merchant_category_xcd"],
    ["rp1_mmhid"],
    ["clean_aggregate_merchant_name"],
]

# List of columns selected from rp1-summarized-txns table.
# These columns act as the input to transitivity mapper.
TXN_DESRCIPTIONS_SUMMARY_COLS = [
    "dp4_merchant_name",
    "dp4_merchant_state",
    "dp4_merchant_city_name",
    "dp4_merchant_category_xcd",
    "vendor_primary_merchant",
    "vendor_intermediaries",
    "vendor_channel",
    "join_methodology",
    "rp1_original_name_trimmed",
    "rp1_mmhid",
    "rp1_merchant_name_cleansed",
    "rp1_phone_number_cleansed",
    "rp1_address_cleansed",
    "rp1_city_name_cleansed",
    "rp1_state_province_code_cleansed",
    "clean_aggregate_merchant_name",
    "clean_parent_aggregate_merchant_name",
    "n_txns",
    "n_members",
    "sum_transaction_amt",
    "sum_purchase_amt",
    "first_seen",
    "last_seen",
]
