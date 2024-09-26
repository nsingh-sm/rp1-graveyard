# rp1-tagging-rec-precompute Data Plane

This data-plane computes tagging opportunities using RP1 data. It outputs the following table:

2. rp1_transitive_tagging_recommendations: Enlists all BSM transaction descriptions that are recommended to a different vendor_primary_merchant via transitivity mapping. Transitivity mapping is done using 3 feature fields:
- rp1_mmhid, dp4_mcc
- rp1_mmhid
- clean_aggregate_merchant_name

## Change Log

Version 1.2.1 : 2024-05-23:
- Filter out duplicate brand recommendations that arise due to different transitive features.

Version 1.2.0 : 2024-05-01:
- Upgrade to pyspark3.4
- Deprecate rp1_simple_tagging_recommendations in favor of rp1_transaction_descriptions_summary table.
- Work off rp1_transaction-descriptions-summary table instead of rp1_enriched_txns table.
- Shift cleaning of rp1_aggregate_merchant_name and rp1_parent_aggregate_merchant_name to rp1_standardization.
- Rename src.rp1_tagging_precompute_rec file to src.transitivity_mapper.
- Move non-transitivity_mapper helper functions to src.utils.

Version 1.1.0 : 2024-04-25:
- Filter out tagging recommendation collisions between brands that are subsidiaries of the same company.

## Development

When cloned and within project directory, run:

> `smctl data-plane init`

To setup virtual environment.

### Add Python Dependencies

> `poetry add <package_name>`

### Update Python Dependencies

> `poetry update`

### Build container

> `smctl data-plane build`

### Run Data Plane

> `smctl data-plane run`
