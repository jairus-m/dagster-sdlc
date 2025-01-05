select
    all_ride_totals__count
    , {{ unit_conversion('all_ride_totals__distance', 'meters', 'miles') }} as all_ride_totals__distance_miles
    , {{ unit_conversion('all_ride_totals__elapsed_time', 'seconds', 'days') }} as all_ride_totals__elapsed_time_days
    , {{ unit_conversion('all_ride_totals__elevation_gain', 'meters', 'miles') }} as all_ride_totals__elevation_gain_miles
    , {{ unit_conversion('all_ride_totals__moving_time', 'seconds', 'days') }} as all_ride_totals__moving_time_days

    , all_run_totals__count
    , {{ unit_conversion('all_run_totals__distance', 'meters', 'miles') }} as all_run_totals__distance_miles
    , {{ unit_conversion('all_run_totals__elapsed_time', 'seconds', 'days') }} as all_run_totals__elapsed_time_days
    , {{ unit_conversion('all_run_totals__elevation_gain', 'meters', 'miles') }} as all_run_totals__elevation_gain_miles
    , {{ unit_conversion('all_run_totals__moving_time', 'seconds', 'days') }} as all_run_totals__moving_time_days

    , all_swim_totals__count
    , {{ unit_conversion('all_swim_totals__distance', 'meters', 'miles') }} as all_swim_totals__distance_miles
    , {{ unit_conversion('all_swim_totals__elapsed_time', 'seconds', 'hours') }} as all_swim_totals__elapsed_time_hours
    , {{ unit_conversion('all_swim_totals__elevation_gain', 'meters', 'feet') }} as all_swim_totals__elevation_gain_feet
    , {{ unit_conversion('all_swim_totals__moving_time', 'seconds', 'hours') }} as all_swim_totals__moving_time_hours

    , {{ unit_conversion('biggest_climb_elevation_gain', 'meters', 'feet') }} as biggest_climb_elevation_gain_feet
    , {{ unit_conversion('biggest_ride_distance', 'meters', 'miles') }} as biggest_ride_distance_miles

    , recent_ride_totals__achievement_count
    , recent_ride_totals__count
    , {{ unit_conversion('recent_ride_totals__distance', 'meters', 'miles') }} as recent_ride_totals__distance_miles
    , {{ unit_conversion('recent_ride_totals__elapsed_time', 'seconds', 'hours') }} as recent_ride_totals__elapsed_time_hours
    , {{ unit_conversion('recent_ride_totals__elevation_gain', 'meters', 'feet') }} as recent_ride_totals__elevation_gain_feet
    , {{ unit_conversion('recent_ride_totals__moving_time', 'seconds', 'hours') }} as recent_ride_totals__moving_time_hours

    , recent_run_totals__achievement_count
    , recent_run_totals__count
    , {{ unit_conversion('recent_run_totals__distance', 'meters', 'miles') }} as recent_run_totals__distance_miles
    , {{ unit_conversion('recent_run_totals__elapsed_time', 'seconds', 'hours') }} as recent_run_totals__elapsed_time_hours
    , {{ unit_conversion('recent_run_totals__elevation_gain', 'meters', 'feet') }} as recent_run_totals__elevation_gain_feet
    , {{ unit_conversion('recent_run_totals__moving_time', 'seconds', 'hours') }} as recent_run_totals__moving_time_hours

    , recent_swim_totals__achievement_count
    , recent_swim_totals__count
    , {{ unit_conversion('recent_swim_totals__distance', 'meters', 'miles') }} as recent_swim_totals__distance_miles
    , {{ unit_conversion('recent_swim_totals__elapsed_time', 'seconds', 'minutes') }} as recent_swim_totals__elapsed_time_minutes
    , {{ unit_conversion('recent_swim_totals__elevation_gain', 'meters', 'feet') }} as recent_swim_totals__elevation_gain_feet
    , {{ unit_conversion('recent_swim_totals__moving_time', 'seconds', 'minutes') }} as recent_swim_totals__moving_time_minutes

    , ytd_ride_totals__count
    , {{ unit_conversion('ytd_ride_totals__distance', 'meters', 'miles') }} as ytd_ride_totals__distance_miles
    , {{ unit_conversion('ytd_ride_totals__elapsed_time', 'seconds', 'days') }} as ytd_ride_totals__elapsed_time_days
    , {{ unit_conversion('ytd_ride_totals__elevation_gain', 'meters', 'feet') }} as ytd_ride_totals__elevation_gain_feet
    , {{ unit_conversion('ytd_ride_totals__moving_time', 'seconds', 'days') }} as ytd_ride_totals__moving_time_days

    , ytd_run_totals__count
    , {{ unit_conversion('ytd_run_totals__distance', 'meters', 'miles') }} as ytd_run_totals__distance_miles
    , {{ unit_conversion('ytd_run_totals__elapsed_time', 'seconds', 'days') }} as ytd_run_totals__elapsed_time_days
    , {{ unit_conversion('ytd_run_totals__elevation_gain', 'meters', 'feet') }} as ytd_run_totals__elevation_gain_feet
    , {{ unit_conversion('ytd_run_totals__moving_time', 'seconds', 'days') }} as ytd_run_totals__moving_time_days

    , ytd_swim_totals__count
    , {{ unit_conversion('ytd_swim_totals__distance', 'yards', 'miles') }} as ytd_swim_totals__distance_miles
    , {{ unit_conversion('ytd_swim_totals__elapsed_time', 'seconds', 'minutes') }} as ytd_swim_totals__elapsed_time_minutes
    , {{ unit_conversion('ytd_swim_totals__elevation_gain', 'meters', 'feet') }} as ytd_swim_totals__elevation_gain_feet
    , {{ unit_conversion('ytd_swim_totals__moving_time', 'seconds', 'minutes') }} as ytd_swim_totals__moving_time_minutes
from {{ ref('stg_stats') }}
