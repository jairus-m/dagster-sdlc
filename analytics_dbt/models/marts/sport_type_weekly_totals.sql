select
    da.week_start
    , da.sport_type
    , sum(distance_miles) as distance_miles
    , sum(moving_time_minutes) as moving_time_minutes
    , sum(elapsed_time_minutes) as elapsed_time_minutes
    , sum(total_elevation_gain_feet) as total_elevation_gain_feet
    , sum(achievement_count) as achievement_count
    , sum(kudos_count) as kudos_count
    , sum(comment_count) as comment_count
    , sum(pr_count) as pr_count
from {{ ref('fct_activities') }} as fa
left join {{ ref('dim_activities') }} as da
    on fa.id = da.id
group by week_start, sport_type
order by week_start desc
