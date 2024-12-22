with 

cycling_totals as (
    select count(*) as total_activities 
    from {{ ref('fct_activities') }} as fct
    left join {{ ref('dim_activities') }} as dim
        on fct.id = dim.id
    where sport_type = 'Cycling'
     and average_watts is not null
)

select * 
from cycling_totals
where total_activities < 1000
