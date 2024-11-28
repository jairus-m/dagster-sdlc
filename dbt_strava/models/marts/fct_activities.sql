with activities as (
    select
        date,
        time,
        distance_miles,
        moving_time_minutes,
        elapsed_time_minutes,
        total_elevation_gain_feet,
        sport_type,
        id,
        achievement_count,
        kudos_count,
        comment_count,
        athlete_count, 
        private,
        visibility,
        average_speed_mph,
        max_speed_mph,
        has_heartrate,
        pr_count,
        average_cadence,
        average_temp,
        average_watts,
        max_watts,
        weighted_average_watts,
        kilojoules,
        average_heartrate,
        max_heartrate,
        elev_high_feet,
        elev_low_feet
    from {{ ref('obt_clean_activities') }}

),

dates as (
    select * from {{ ref('dim_dates') }}
),

sport_type as (
    select * from {{ ref('dim_sport_type') }}
),

privacy as (
    select * from {{ ref('dim_privacy') }}
),

has_heartrate as (
    select * from {{ ref('dim_has_heartrate') }}
)

select 
    a.id as id_pk,
    d.date_id,
    a.time,
    s.sport_type_pk as sport_type_fk,
    p.visibility_pk as visibility_fk,
    a.distance_miles,
    a.moving_time_minutes,
    a.elapsed_time_minutes,
    a.total_elevation_gain_feet,
    a.achievement_count,
    a.kudos_count,
    a.comment_count,
    a.athlete_count, 
    a.average_speed_mph,
    a.max_speed_mph,
    a.pr_count,
    a.average_cadence,
    a.average_temp,
    a.average_watts,
    a.max_watts,
    a.weighted_average_watts,
    a.kilojoules,
    a.average_heartrate,
    a.max_heartrate,
    a.elev_high_feet,
    a.elev_low_feet,
    h.has_heartrate_pk as has_heartrate_fk,
from activities as a
left join dates as d
    on d.date = a.date and d.time = a.time
left join sport_type as s
    on a.sport_type = s.sport_type
left join privacy as p
    on a.private = p.private
left join has_heartrate as h
    on a.has_heartrate = h.has_heartrate
