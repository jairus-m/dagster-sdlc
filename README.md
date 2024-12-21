# The Analytics Development Lifecycle within a Modern Data Engineering Framework

Utilizing dltHub, dbt, + dagster as a framework for developing data products with software engineering best practices. 

![Slide1](https://github.com/user-attachments/assets/0c0843cb-9a6d-41e3-9309-379a21147a5a)


While the short-term goal is to learn these tools, the greater goal is to understand and flesh out what the full development and deployment cycle look like for orchestrating a data platform and deploying custom pipelines. There is a great process using dbt where we have local development, testing, versioning/branching, CICD, code-review, separation of dev and prod, project structure/cohesion etc., but how can we apply that to the entire data platform and espeacially, the 10-20% of ingestion jobs that cannot be done in a managed tool like Airbyte and/or is best done using a custom solution?

# Current Status
<img width="1512" alt="Screenshot 2024-12-13 at 11 00 14 PM" src="https://github.com/user-attachments/assets/a29f1da9-2d6c-46f7-b3ed-3ed6679c88e0" />

- Built a dltHub EL pipeline via the RESTAPIConfig class in `dagster_proj/assets/activities.py`
  - Declaratively extracts my raw activity data from Strava's REST API and loads it into DuckDB
- Built a dbt-core project to transform the staged activities data in `analytics_dbt/models`
- Orchestrated ingest, transformation, and downstream dependecies (ML) with Dagster
  - Developed in dev environment and materaizlied in `dagster dev` server
  - Configured resources / credentials in .env
  - Current Dagster folder structure (dependencies managed by UV)
    - One code location: `dagster_proj/` 
      - Assets: `dagster_proj/assets/`
      - Resources: `dagster_proj/resources/__init__.py`
      - Jobs: `dagster_proj/jobs/__init__.py`
      - Schedules: `dagster_proj/schedules/__init__.py`
      - Definitions: `dagster_proj/__init__.py`
    - The structure is experimental and based on the DagsterU courses
- Created an sklearn ML pipeline to predict energy expenditure for a given cycling activity
  - WIP but the general flow of preprocessing, building the ML model, training, testing/evaluation, and prediction can be found in `dagster_proj/assets/energy_prediction.py`
  - This a downstream dependency of a dbt asset materialized in duckdb

## Deployment Status
- Officially Deployed this project to Dagster+ !!!
  - CICD w/ branching deployments for every PR
- Seperated execution environments into ([implementation details](https://github.com/jairus-m/dagster-dlt/pull/9))
  - dev
  - branch
  - prod
- Added `ruff` Python linter

## TODO:
- Add unit tests
- Add additional CI checks to run unit tests, Python linting, etc
- Beef up the ML pipeline with `dagster-mlflow` for experiment tracking, model versioning, better model observability, etc
- Add new Strava end points / dbt models 

# Getting Started:
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
