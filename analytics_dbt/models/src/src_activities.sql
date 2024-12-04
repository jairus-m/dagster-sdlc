with raw_activities AS (
    SELECT * FROM {{ source('activities_staging', 'activities') }}   
)

SELECT *
FROM raw_activities