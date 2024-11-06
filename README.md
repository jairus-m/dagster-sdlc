# Work In Progress..
## TODO:
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
  - Addresses main problems that we face as an org for EL / dataplatform unification:
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

### The case for an orchestrator: 
- An orchestrator addresses the fragmentation by providing a unified system for managing, monitoring, and orchestrating all data assets and workflows.  
- However, an orchestrator does not address pains of not having a solid software development lifecycle 

### How Dagster can addreses these problems:
- Declarative and asset-based  
- Foundational philosophical/architectural difference with Airflow/Prefect that enables key capabilities that make data engineering teams far more productive 
- Python-first with full support of a mature SDLC  
  - versioning, local development, dev/prod deployment, CICD, branching, code reviews, unified repository organization, etc 
- Fully hosted, serverless solution to execute custom ingestion code (hydrib deployments as well!)
- Integrates well with dbt 
- All the benefits of having an orchestrator for end-to-end observability, logging, testing, and has a built-in data catalog 

## Current Status
- learning the foundations of dlt concepts, configs, classes, and other features by refactoring an old ELT pipeline
- once i have a better grasp, will move on to migrating to a dagster project and flesh out local dev/testing, project stucture, and deployoment
- then will introduce a dagster project with multiple dlt pipelines and dependencies

## Outstanding Questions
- What is the best way to implement logging within a generator?
  - How to create a standard logging object for use across an entire repository
- What does deployment look like for dlt Pipelines?
  - via GitHub actions?
  - via serverless Dagster+?
- How would a Dagster repo with mulitple pipelines and dependecies be organized?
    - Mulitple code locations?
    - One code location?
    - What is the best structure of the repo?
- What would the full analytics development lifecycle look like with these tools?
    - Local dev/testing, CICD, branching, prod deployment, etc
- How does testing work?
    - unit tests
    - type tests
    - data tests
- Will add more as they come