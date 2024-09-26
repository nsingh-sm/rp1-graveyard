DESCRIPTOR_TBL = "descriptors"
LOCATION_TBL = "locations"

COLS_DICT = {}
COLS_DICT[DESCRIPTOR_TBL] = {}
COLS_DICT[LOCATION_TBL] = {}

COLS_DICT[DESCRIPTOR_TBL]["primary_keys"] = ["original_name", "original_state", "original_city"]
COLS_DICT[DESCRIPTOR_TBL]["data_cols"] = ["mmhid"]
COLS_DICT[DESCRIPTOR_TBL]["start"] = "period_start_date"
COLS_DICT[DESCRIPTOR_TBL]["end"] = "period_end_date"

COLS_DICT[LOCATION_TBL]["primary_keys"] = ["mmhid"]
COLS_DICT[LOCATION_TBL]["data_cols"] = [
    "merchant_name",
    "merchant_name_cleansed",
    "legal_corporate_name",
    "legal_corporate_name_cleansed",
    "merchant_category_code",
    "geocoding_quality_indicator",
    "phone_number",
    "phone_number_cleansed",
    "latitude",
    "longitude",
    "address",
    "address_cleansed",
    "postal_code",
    "postal_code_cleansed",
    "city_name",
    "city_name_cleansed",
    "state_province_code",
    "state_province_code_cleansed",
    "country_code",
    "country_code_cleansed",
    "aggregate_merchant_id",
    "aggregate_merchant_name",
    "dun_bradstreet_number",
    "key_aggregate_merchant_id",
    "parent_aggregate_merchant_id",
    "parent_aggregate_merchant_name",
    "msa_code",
    "naics_code",
    "dma_code",
    "industry",
    "super_industry",
]
COLS_DICT[LOCATION_TBL]["start"] = "period_start_date"
COLS_DICT[LOCATION_TBL]["end"] = "period_end_date"
