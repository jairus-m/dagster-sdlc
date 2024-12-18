# The Analytics Development Lifecycle within a Modern Data Engineering Framework

Utilizing dltHub, dbt, + dagster as a framework for developing data products with software engineering best practices. 

![Slide1](https://github.com/user-attachments/assets/0c0843cb-9a6d-41e3-9309-379a21147a5a)


While the short-term goal is to learn these tools, the greater goal is to understand and flesh out what the full development and deployment cycle look like for orchestrating a data platform and deploying custom pipelines. There is a great process using dbt where we have local development, testing, versioning/branching, CICD, code-review, separation of dev and prod, project structure/cohesion etc., but how can we apply that to the entire data platform and espeacially, the 10-20% of ingestion jobs that cannot be done in a managed tool like Airbyte and/or is best done using a custom solution?

# Current Status [12/17/24]
<img width="1512" alt="Screenshot 2024-12-13 at 11 00 14 PM" src="https://github.com/user-attachments/assets/a29f1da9-2d6c-46f7-b3ed-3ed6679c88e0" />

- Officially Deployed this project to Dagster+ !!!
  - CICD w/ branching deployments for every PR
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

## TODO:
- Concretely seperate dev from prod
- Add unittests
- Incorporate a Python linter (like ruff) to make sure code is standardized, neat, and follow PEP8 

## Goals:
- Learn the dlt library to maximize features such as 
    - Automated schema inference / evolution from raw json
    - Declaratively defining data pipelines
    - Type checking / in-flight data validation
    - Understanding resources, sources, and other concepts
    - Using Python generators, decorators
    - Applying SWE best practices to writing ELT code
- Learn duckDB
    - As a force-mulitplier and cost-saver for local development
        - Portable, feature-rich, FAST, and free
- Learn Dagster
  - Addresses main problems that we face as an org for EL / data platform unification:
    - Fragmented tooling and organization of ingestion jobs 
    - No standard deployment or development process for ingestion jobs that cannot be done via Airbyte/Data shares/etc that need to be written in custom code 
      - No version control, CICD, PR review, collaboration, testing, separate deployment environments, local development, etc 
      - Custom code does not live in organized repositories 
    - No orchestration of assets across the entire data pipeline 
      - No end-to-end observability / centralized monitoring 
      - No unified view of data platform 
      - Difficult to assess the health of the platform and debug 
      - Cannot optimize cost/compute  
      - Fragility of ELT execution 
    - Fragmented tooling / development / deployment =  
      - Low throughput 
      - Higher costs 
      - Increased long-term technical debt 
      - Shitty dev experience  


### How Dagster can addreses these problems:
- Declarative and asset-based  
- Foundational philosophical/architectural difference with Airflow/Prefect that enables key capabilities that make data engineering teams far more productive 
- Python-first with full support of a mature SDLC  
  - versioning, local development, dev/prod deployment, CICD, branching, code reviews, unified repository organization, etc 
- Fully hosted, serverless solution to execute custom ingestion code (hydrib deployments as well!)
- Integrates well with dbt 
- All the benefits of having an orchestrator for end-to-end observability, logging, testing, and has a built-in data catalog 


# Getting Started:
1. Clone this repo locally
2. Create a `.env` file at the root of the directory:
  ```
  DUCKDB_DATABASE=data/staging/strava.duckdb
  DAGSTER_ENVIRONMENT=dev
  DBT_PROFILES_DIR=/Users/FULL_PATH_TO_CLONED_REPO/analytics_dbt

  #strava
  CLIENT_ID= 
  CLIENT_SECRET=
  REFRESH_TOKEN=
  ```
3. Download `uv` and run `uv sync`
4. Build the Python package in developer mode via `uv pip install -e ".[dev]"`
5. Run the dagster daemon locally via `dagster dev`
6. Materialize the pipeline!

Note: The `refresh_token` in the Strava UI produces an `access_token` that is limited in scope. Please follow these [Strava Dev Docs](https://developers.strava.com/docs/getting-started/#oauth) to generate the proper `refresh_token` which will then produce an `access_token` with the proper scopes. 
