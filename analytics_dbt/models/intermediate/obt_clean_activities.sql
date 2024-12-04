{{ config(materialized = 'table') }}

WITH src_activities AS (
  SELECT * 
  FROM {{ ref('src_activities') }}
)

SELECT  
    strftime(start_date_local::TIMESTAMP, '%m-%d-%Y') as date,
    strftime(start_date_local::TIMESTAMP, '%H:%M:%S %p') as time,
    name,
    (distance * 0.000621371) AS distance_miles,
    (moving_time / 60) AS moving_time_minutes,
    (elapsed_time / 60) AS elapsed_time_minutes,
    (total_elevation_gain * 3.28084) AS total_elevation_gain_feet,
    sport_type,
    id,
    achievement_count,
    kudos_count,
    comment_count,
    athlete_count, 
    private,
    visibility,
    (average_speed * 2.23694) AS average_speed_mph,
    (max_speed * 2.23694) AS max_speed_mph,
    has_heartrate,
    pr_count,
    has_kudoed,
    average_cadence,
    average_temp,
    average_watts,
    max_watts,
    weighted_average_watts,
    kilojoules,
    average_heartrate,
    max_heartrate,
    (elev_high * 3.28084) AS elev_high_feet,
    (elev_low * 3.28084) AS elev_low_feet,
    CASE
        WHEN date_part('hour', start_date_local::TIMESTAMP) BETWEEN 0 AND 3 THEN '12am-4am'
        WHEN date_part('hour', start_date_local::TIMESTAMP) BETWEEN 4 AND 7 THEN '4am-8am'
        WHEN date_part('hour', start_date_local::TIMESTAMP) BETWEEN 8 AND 11 THEN '8am-12pm'
        WHEN date_part('hour', start_date_local::TIMESTAMP) BETWEEN 12 AND 15 THEN '12pm-4pm'
        WHEN date_part('hour', start_date_local::TIMESTAMP) BETWEEN 16 AND 19 THEN '4pm-8pm'
        WHEN date_part('hour', start_date_local::TIMESTAMP) BETWEEN 20 AND 23 THEN '8pm-12am'
        ELSE 'Other'
    END AS time_bin
FROM src_activities
ORDER BY start_date_local DESC