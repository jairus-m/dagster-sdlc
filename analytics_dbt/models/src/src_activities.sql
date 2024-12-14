with raw_activities AS (
    SELECT * FROM {{ source('strava', 'activities') }}   
)

SELECT *
FROM raw_activities