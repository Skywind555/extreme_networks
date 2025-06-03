with base as (
    select
        source_id,
        company_name,
        territory,
        update_date
    from {{ ref('stg_customer') }}
),

territory_change_flags as (
    select
        *,
        case
            when lag(territory) over (partition by source_id order by update_date) is null
                then 1
            when territory != lag(territory) over (partition by source_id order by update_date)
                then 1
            else 0
        end as territory_change
    from base
),

territory_groups as (
    select
        *,
        sum(territory_change) over (
            partition by source_id
            order by update_date
            rows between unbounded preceding and current row
        ) as territory_group
    from territory_change_flags
),


scd2_periods as (
    select
        source_id,
        min(update_date) as start_date,
        any_value(company_name) as company_name,
        any_value(territory) as territory
    from territory_groups
    group by source_id, territory_group
),

final as (
    select
        *,
        lead(start_date) over (
            partition by source_id
            order by start_date
        ) as end_date
    from scd2_periods
)

select
    source_id,
    company_name,
    territory,
    start_date,
    end_date
from final
