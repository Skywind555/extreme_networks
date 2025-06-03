with fact as (
  select * from {{ ref('fact_revenue') }}
),

customer as (
  select * from {{ ref('dim_customer') }}
),

forecast_revenue as (
  select * from {{ source('seeds', 'revenue_forecast') }}
)

select
  f.date,
  d.year,
  d.quarter,
  f.source_id,
  c.company_name,
  c.territory,
  f.revenue as actual_revenue,
  fr.revenue as forecast_revenue,
  fr.revenue - coalesce(f.revenue, 0) as revenue_difference
from fact as f
left join forecast_revenue as fr
  on f.source_id = fr.source_id
  and f.date = fr.date
inner join customer as c
  on f.source_id = c.source_id
  and f.date >= c.start_date
  and (f.date < c.end_date or c.end_date is null)
inner join {{ ref('dim_date') }} as d
  on f.date = d.date
