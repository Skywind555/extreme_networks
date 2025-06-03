select * 
from {{ source('seeds', 'date_spine') }}
