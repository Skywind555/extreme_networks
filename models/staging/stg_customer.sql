{{ config(
    materialized='incremental',
    unique_key=['source_id', 'update_date'],
    incremental_strategy='merge'
) }}

with latest_source as (
    select
        cast(source_id as int64) as source_id,
        company_name,
        territory,
        PARSE_DATE('%m/%d/%Y', create_date) as create_date,
        PARSE_DATE('%m/%d/%Y', update_date) as update_date
    from {{ source('source', 'customer') }}
)

{% if is_incremental() %}
, existing_max_version as (
    select
        source_id,
        max(version) as last_version,
        max(update_date) as last_update_date
    from {{ this }}
    group by source_id
)

, new_versions as (
    select
        s.*,
        coalesce(e.last_version, 0) + 1 as version
    from latest_source s
    left join existing_max_version e
        on s.source_id = e.source_id
    where e.last_update_date is null or s.update_date > e.last_update_date
)
select
    source_id,
    company_name,
    territory,
    create_date,
    update_date,
    version
from new_versions

{% else %}
, all_versions as (
    select
        *,
        row_number() over (partition by source_id order by update_date) as version
    from latest_source
)
select
    source_id,
    company_name,
    territory,
    create_date,
    update_date,
    version
from all_versions
{% endif %}
