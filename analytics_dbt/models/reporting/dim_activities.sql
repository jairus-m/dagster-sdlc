select 
    id -- pk
    , name
    , has_heartrate
    , private
    , visibility
    , all_sport_types
    , sport_type
    , week_start
    , day_of_week_num
    , day_of_week
    , week_num
    , month_num
    , month
from {{ ref('src_activities') }}
