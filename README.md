 # OpenSky DataOps Telemetry Pipeline

An automated, production-grade DataOps pipeline for ingesting, transforming, and validating real-time flight telemetry state vectors from the [OpenSky Network API](https://opensky-network.org/). The project uses a Databricks Delta Lakehouse with Medallion Architecture, Python, dbt Core, and GitHub Actions.

## System Architecture

```text
[ OpenSky REST API ]
		 │
		 ▼
[ Python Ingestion Script ] ──(Databricks SDK)──> [ Databricks Volume ]
														  │
														  ▼ (COPY INTO pre-hook)
											 [ Bronze Layer: Delta Raw ]
														  │
														  ▼ (dbt table + windowing)
											 [ Silver Layer: stg_flights ]
														  │
														  ▼ (dbt incremental watermark)
											 [ Gold Layer: fct_country_kpis ]
														  │
														  ▼ (dbt test)
											[ Quality & Governance Checks ]
```

## Repository Structure

```text
skynet-dataops-pipeline/
├── .github/workflows/daily_dataops_pipeline.yml
├── dbt_flight_ops/
│   ├── models/
│   │   ├── sources.yml
│   │   ├── schema.yml
│   │   ├── stg_flights.sql
│   │   └── fct_country_kpis.sql
│   ├── dbt_project.yml
│   └── profiles.yml
├── data/raw/                  # Local landed JSON batches (git-ignored)
├── extraction.py
├── requirements.txt
└── README.md
```

## Medallion Lakehouse

The pipeline processes data in Databricks Unity Catalog under the `skynet` catalog:

- **Landing Volume:** `/Volumes/skynet/default/raw_open_sky_landing/` receives timestamped JSON payloads through `databricks-sdk`.
- **Bronze:** `skynet.default.flight_ops_bronze` is populated by a dbt `COPY INTO` pre-hook.
- **Silver:** `skynet.default.stg_flights` enforces the schema and deduplicates state vectors with `ROW_NUMBER()` over `(icao24, time_position)`, retaining the latest ingestion.
- **Gold:** `skynet.default.fct_country_kpis` incrementally aggregates metrics by `origin_country` and flight status using an ingestion watermark.

## Data Quality & Governance

`dbt test` runs automatically on every pipeline execution:

- Unique `kpi_surrogate_key` values in the gold model.
- Composite uniqueness across `origin_country` and `on_ground` using `dbt_utils.unique_combination_of_columns`.
- `not_null` constraints for `icao24`, `latitude`, `longitude`, and `origin_country`.

## Automated CI/CD

The workflow at `.github/workflows/daily_dataops_pipeline.yml` runs extraction, transformation, and testing.

### Triggers

- `push`: runs on every push to `main`.
- `schedule`: runs daily at `06:00 UTC` (`0 6 * * *`).
- `workflow_dispatch`: supports manual execution from the GitHub Actions UI.
