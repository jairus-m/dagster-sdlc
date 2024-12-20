select 
    id
    , name
    , has_heartrate
    , private
    , visibility
    , case 
        when sport_type in ('VirtualRide', 'Ride', 'MountainBikeRide') then 'Cycling'
        when sport_type = 'Run' then 'Running'
        else 'Other'
    end as sport_type
from {{ ref('obt_clean_activities') }}
