with src_activities AS (
  select * 
  from {{ ref('stg_activities') }}
)

select  
  {{ get_date('start_date::timestamp') }} as date,
  {{ get_time('start_date::timestamp') }} as time,
  name,
  {{ unit_conversion('distance', 'meters', 'miles') }} as distance_miles,
  {{ unit_conversion('moving_time', 'seconds', 'minutes') }} as moving_time_minutes,
  {{ unit_conversion('elapsed_time', 'seconds', 'minutes') }} as elapsed_time_minutes,
  {{ unit_conversion('total_elevation_gain', 'meters', 'feet') }} as total_elevation_gain_feet,
  sport_type as all_sport_types,
  id,
  achievement_count,
  kudos_count,
  comment_count,
  athlete_count,
  private,
  visibility,
  {{ unit_conversion('average_speed', 'mps', 'mph') }} as average_speed_mph,
  {{ unit_conversion('max_speed', 'mps', 'mph') }} as max_speed_mph,
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
  {{ unit_conversion('elev_high', 'meters', 'feet') }} as elev_high_feet,
  {{ unit_conversion('elev_low', 'meters', 'feet') }} as elev_low_feet,
  case
    when {{ date_part('hour', 'start_date::timestamp') }} between 0 AND 3 then '12am-4am'
    when {{ date_part('hour', 'start_date::timestamp') }} between 4 AND 7 then '4am-8am'
    when {{ date_part('hour', 'start_date::timestamp') }} between 8 AND 11 then '8am-12pm'
    when {{ date_part('hour', 'start_date::timestamp') }} between 12 AND 15 then '12pm-4pm'
    when {{ date_part('hour', 'start_date::timestamp') }} between 16 AND 19 then '4pm-8pm'
    when {{ date_part('hour', 'start_date::timestamp') }} between 20 AND 23 then '8pm-12am'
    else 'Other'
  end as time_bin,
  case 
    when sport_type in ('VirtualRide', 'Ride', 'MountainBikeRide') then 'Cycling'
    when sport_type = 'Run' then 'Running'
    else 'Other'
  end as sport_type,
  {{ date_trunc('week', 'date') }} AS week_start,
  {{ date_part('dow', 'date') }} + 1 AS day_of_week_num,
  {{ strftime('date', "'%A'") }} AS day_of_week,  
  {{ date_part('week','date') }} AS week_num, 
  {{ date_part('month', 'date') }} AS month_num,  
  {{ strftime('date', "'%B'") }} AS month
from src_activities
order by start_date_local desc
