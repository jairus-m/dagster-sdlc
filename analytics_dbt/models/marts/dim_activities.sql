select 
    id -- pk
    , name
    , has_heartrate
    , private
    , visibility
    , sport_type as all_sport_types
    , case 
        when sport_type in ('VirtualRide', 'Ride', 'MountainBikeRide') then 'Cycling'
        when sport_type = 'Run' then 'Running'
        else 'Other'
    end as sport_type
    -- date transforms
    , date_trunc('week', strptime(date, '%m-%d-%Y')) AS week_start
    , extract('dow' FROM strptime(date, '%m-%d-%Y')) + 1 AS day_of_week_num
    , strftime(strptime(date, '%m-%d-%Y'), '%A') AS day_of_week
    , extract('week' FROM strptime(date, '%m-%d-%Y')) AS week_num
    , extract('month' FROM strptime(date, '%m-%d-%Y')) AS month_num
    , strftime(strptime(date, '%m-%d-%Y'), '%B') AS month
from {{ ref('obt_clean_activities') }}
