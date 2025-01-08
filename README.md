# The Analytics Development Lifecycle within a Modern Data Engineering Framework

Utilizing dltHub, dbt, + Dagster as a framework for developing data products with software engineering best practices. 

![Slide1](https://github.com/user-attachments/assets/4f0025b5-c203-424f-96a6-5be81f61c844)

While the short-term goal is to learn these tools, the greater goal is to understand and flesh out what the full development and deployment cycle can look like for orchestrating a data platform and deploying custom pipelines. There is a great process in the transformation layer using dbt where we have local development, testing, versioning/branching, CICD, code-review, separation of dev and prod, project structure/cohesion etc., but how can we apply that to the entire data platform and espeacially, the 10-20% of ingestion jobs that cannot be done in a managed tool like Airbyte and/or is best done using a custom solution?

# Current Status
<img width="1317" alt="Screenshot 2025-01-05 at 11 51 00 AM" src="https://github.com/user-attachments/assets/a40c230d-1634-46ca-9210-d7847f487323" />

### Dagster
- Orchestrated ingest, transformation, and downstream dependecies (ML/Analytics) with Dagster - https://github.com/jairus-m/dagster-dlt/pull/2, https://github.com/jairus-m/dagster-dlt/pull/6
  - Developed in dev environment and materaizlied in `dagster dev` server
  - Configured resources / credentials in a root `.env` file
  - Current Dagster folder structure (dependencies managed by UV) - https://github.com/jairus-m/dagster-dlt/pull/15
    - One code location: `dagster_proj/` 
      - Assets: `dagster_proj/assets/`
      - Resources: `dagster_proj/resources/__init__.py`
      - Jobs: `dagster_proj/jobs/__init__.py`
      - Schedules: `dagster_proj/schedules/__init__.py`
      - Utils: `dagster_proj/utils/__init__.py`
      - Definitions: `dagster_proj/__init__.py`
    - The structure is experimental and based on the DagsterU courses
### dltHub
- Built a dltHub EL pipeline via the RESTAPIConfig class in `dagster_proj/assets/dlt/activities.py`
  - Declaratively extracts my raw activity/stats data from Strava's REST API and loads it into DuckDB/Snowflake
    - Added mulitple Strava endpoints - https://github.com/jairus-m/dagster-dlt/pull/18
  - Created a custom configurable resource for Strava API - https://github.com/jairus-m/dagster-dlt/pull/5, https://github.com/jairus-m/dagster-dlt/pull/11
### dbt-core
- Built a dbt-core project to transform the activities data in `analytics_dbt/models`
### Sklearn ML Pipeline
- Created an Sklearn ML pipeline to predict energy expenditure for a given cycling activity
  - WIP but the general flow of preprocessing, building the ML model, training, testing/evaluation, and prediction can be found in `dagster_proj/assets/ml_analytics/energy_prediction.py`
  - This a downstream dependency of a dbt asset materialized in duckdb
### Analytics
- Created a Plotly analytics dashboard + an ML results related visulization - https://github.com/jairus-m/dagster-dlt/pull/14
  - In `dagster_proj/assets/ml_analytics/weekly_totals.py` 

## Deployment Status
- Deployed this project to Dagster+ 
  - CICD w/ branching deployments for every PR
- Seperated execution environments - https://github.com/jairus-m/dagster-dlt/pull/13
  - dev (DuckDB)
  - branch (Snowflake)
  - prod (Snowflake)
- Configured pre-commits / CI checks and added unit tests - https://github.com/jairus-m/dagster-dlt/pull/16
  - Added `ruff` Python linter - https://github.com/jairus-m/dagster-dlt/pull/8
  - Astral `uv` for Python dependency management - https://github.com/jairus-m/dagster-dlt/pull/1

## TODO:
- Beef up the ML pipeline with `dagster-mlflow` for experiment tracking, model versioning, better model observability, etc
- Utilize Snowflake Cloning/dbt Slim CI for CI
- Implement partitions/backfilling with dlt/Dagster

# Getting Started:

For local development only:

1. Clone this repo locally
2. Create a `.env` file at the root of the directory:
  ```
  # these are the config values for local dev and will change in branch/prod deployment
  DBT_TARGET=dev
  DAGSTER_ENVIRONMENT=dev
  DUCKDB_DATABASE=data/dev/strava.duckdb

  #strava
  CLIENT_ID= 
  CLIENT_SECRET=
  REFRESH_TOKEN=
  ```
3. Download `uv` and run `uv sync`
4. Build the Python package in developer mode via `uv pip install -e ".[dev]"`
5. Run the dagster daemon locally via `dagster dev`
6. Materialize the pipeline!

__Additional Notes:__ 
- The `refresh_token` in the Strava UI produces an `access_token` that is limited in scope. Please follow these [Strava Dev Docs](https://developers.strava.com/docs/getting-started/#oauth) to generate the proper `refresh_token` which will then produce an `access_token` with the proper scopes.
- If you want to run the dbt project locally, outside of dagster, you need to add a `DBT_PROFILES_DIR` environment variable to the .env file and export it
  - For example, my local env var is: `DBT_PROFILES_DIR=/Users/jairusmartinez/Desktop/dlt-strava/analytics_dbt`
  - Yours will be: `DBT_PROFILES_DIR=/PATH_TO_YOUR_CLONED_REPO_DIR/analytics_dbt`
