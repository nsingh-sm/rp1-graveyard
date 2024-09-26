# rp1-transaction-enrichment Data Plane

Join dp4 tranactions with rp1 metadata.

## Change Log:

Version 1.2.0: May 01, 2024

- Rename output tables:
  - rp1-enriched-txns --> rp1_enriched_transactions
  - rp1-summarized_txns --> rp1_transaction_descriptions_summary
  - rp1_summarized_txns_monthly --> rp1_transaction_descriptions_summary_monthly
- Add all columns from enriched_transactions to rp1_enriched_transactions output table.
- Retain only description identifiers as groupby keys for summarized versions of tables.
- Leverage clean_aggregate_merchant_name and clean_parent_aggregate_merchant_name instead of vanilla rp1 variants.
- Rename rp1_summarized_txns to rp1_txns_descriptions_summary and rp1_summarized_txns_monthly to rp1_txns_descriptions_summary_monthly.

Version 1.1.0: Apr 23, 2024

- Upgrade rp1-transaction-enrichment data-plane to pyspark 3.4
- Shift dependency from tagged_transactions to enriched_transactions
- Rename output tables to have the "rp1_" prefix for better semantic consistency
- Modify output tables' schemas for optimized operations.
- Add support for a description grain and monthly description grain summarized version of rp1 enriched transactions
- Stop persisting intermediate tables on s3 in order to reduce i/o
- Reduce logging to enable faster execution

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
