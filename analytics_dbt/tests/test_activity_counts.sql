with 

activity_totals as (
    select count(*) as total_activities from {{ ref('fct_activities') }}
)

select * 
from activity_totals
where total_activities < 2000 
  