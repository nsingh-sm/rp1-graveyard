# rp1-ingestion Data Plane

Ingestion data-plane for ingesting RP1's Places dataset. Slides explaining design and more available [here](https://docs.google.com/presentation/d/18Pu09WEgGU-G4IREGmFzt5xvGo4-PRWm4LAo7eBAEr8/edit#slide=id.g1e5b6029efa_0_1762).

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
