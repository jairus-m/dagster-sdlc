from dagster import define_asset_job, AssetSelection

activities_update_job = define_asset_job(
    name="activities_update_job", selection=AssetSelection.all()
)
