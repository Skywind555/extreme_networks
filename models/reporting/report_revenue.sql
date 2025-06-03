select
  year,
  quarter,
  company_name,
  territory,
  sum(actual_revenue) as revenue,
  sum(forecast_revenue) as forecast_revenue,
  sum(revenue_difference) as revenue_difference
from {{ ref('sem_revenue') }}
group by 1,2,3,4
