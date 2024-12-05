from pathlib import Path

from dagster_dbt import DbtProject

dbt_project = DbtProject(
  project_dir=Path(__file__).joinpath("..", "..", "analytics_dbt").resolve(),
)

dbt_project.prepare_if_dev()

project_dir = str(Path(__file__).parent.parent.joinpath("analytics_dbt").resolve())