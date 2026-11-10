{% set start_date = var("start_date") %}
{% set end_date = var("end_date") %}

{% set source_tables = [
    'stg_coaching_report'
    , 'stg_coach_the_coach'
] %}

with new_data as (

    {%- for source_table in source_tables %}

    select
        {{ loom_macros.bk_varchar(['report_id', 'tenant']) }} as bk_coaching_session
        , {{ loom_macros.hash_key(['report_id', 'tenant']) }} as hk_coaching_session
        , edw_dt_load
        , '{{ ref(source_table, v=2) }}' as edw_record_source
    from {{ ref(source_table, v=2) }}
    where edw_dt_load between {{ start_date }} and {{ end_date }}

    {% if not loop.last %}union all{% endif %}

    {%- endfor %}

)

select * from new_data
