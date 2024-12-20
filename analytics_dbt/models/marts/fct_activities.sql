select 
    id
    , date
    , time
    , distance_miles
    , moving_time_minutes
    , elapsed_time_minutes
    , total_elevation_gain_feet
    , achievement_count
    , kudos_count
    , comment_count
    , athlete_count 
    , average_speed_mph
    , max_speed_mph
    , pr_count
    , average_cadence
    , average_temp
    , average_watts
    , max_watts
    , weighted_average_watts
    , kilojoules
    , average_heartrate
    , max_heartrate
    , elev_high_feet
    , elev_low_feet
from {{ ref('obt_clean_activities') }}
