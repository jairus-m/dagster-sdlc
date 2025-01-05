select *
from {{ source('strava', 'stats') }}