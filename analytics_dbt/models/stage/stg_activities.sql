with raw_activities as (
    select * from {{ source('strava', 'activities') }}   
)

select *
from raw_activities
