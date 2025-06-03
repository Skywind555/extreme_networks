select
    cast(src.source_id as int64) as source_id,
    PARSE_DATE('%m/%d/%Y', src.date) as date,
    cast(src.revenue as float64) as revenue
from {{ source('source', 'revenue') }} as src
