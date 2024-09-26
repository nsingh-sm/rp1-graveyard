# rp1-standardization Data Plane

Standardization data-plane for RP1's Places dataset after ingestion. Slides explaining design and more available [here](https://docs.google.com/presentation/d/18Pu09WEgGU-G4IREGmFzt5xvGo4-PRWm4LAo7eBAEr8/edit#slide=id.g1e5b6029efa_0_1708).

Data Nuances:

1. A strong assumption made while developing this data-plane is that a descriptor - merchant_market_hierarchy_id mapping is valid for all dates between 2 deliveries. This is our best estimate based on the data we receive.
2. RP1 enforces an artificial constraint on the descriptor-mmhid mapping to be 1:1 in every delivery. While we are aware of this, this data-plane has been designed with the same artificial constraint. This shall be revisited in the future once RP1 can provide several mmhids for a descriptor in the same delivery.

## Change Log:

Version 1.1.0: 2024-05-01
- Upgrade to pyspark3.4.
- Add clean_aggregate_merchant_name and clean_parent_aggregate_merchant_name columns.
- Re-format code to impute state and city after (instead of during) standardization of descriptors.
- Improved format of unit-tests for compression-tools.
- Rename input variable names from ingested-rp1-descriptors/locations to raw-rp1-descriptors/locations to be consistent with the rp1-ingestion data-plane.

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
