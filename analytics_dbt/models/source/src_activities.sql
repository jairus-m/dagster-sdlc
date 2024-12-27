with src_activities AS (
  select * 
  from {{ ref('stg_activities') }}
)

select  
  {{ strftime('start_date_local::timestamp', "'%m-%d-%Y'") }} as date,
  {{ strftime('start_date_local::timestamp', "'%H:%M:%S %p'") }} as time,
  name,
  (distance * 0.000621371) AS distance_miles,
  (moving_time / 60) AS moving_time_minutes,
  (elapsed_time / 60) AS elapsed_time_minutes,
  (total_elevation_gain * 3.28084) AS total_elevation_gain_feet,
  sport_type as all_sport_types,
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
  case
    when {{ date_part('hour', 'start_date_local::timestamp') }} between 0 AND 3 then '12am-4am'
    when {{ date_part('hour', 'start_date_local::timestamp') }} between 4 AND 7 then '4am-8am'
    when {{ date_part('hour', 'start_date_local::timestamp') }} between 8 AND 11 then '8am-12pm'
    when {{ date_part('hour', 'start_date_local::timestamp') }} between 12 AND 15 then '12pm-4pm'
    when {{ date_part('hour', 'start_date_local::timestamp') }} between 16 AND 19 then '4pm-8pm'
    when {{ date_part('hour', 'start_date_local::timestamp') }} between 20 AND 23 then '8pm-12am'
    else 'Other'
  end as time_bin,
  case 
    when sport_type in ('VirtualRide', 'Ride', 'MountainBikeRide') then 'Cycling'
    when sport_type = 'Run' then 'Running'
    else 'Other'
  end as sport_type,
  {{ date_trunc('week', strptime('date', "'%m-%d-%Y'")) }} AS week_start, 
  {{ date_part('dow', strptime('date', "'%m-%d-%Y'")) }} + 1 AS day_of_week_num,
  {{ strftime(strptime('date', "'%m-%d-%Y'"), "'%A'") }} AS day_of_week, 
  {{ date_part('week', strptime('date', "'%m-%d-%Y'")) }} AS week_num, 
  {{ date_part('month', strptime('date', "'%m-%d-%Y'")) }} AS month_num, 
  {{ strftime(strptime('date', "'%m-%d-%Y'"), "'%B'") }} AS month   
from src_activities
order by start_date_local desc
