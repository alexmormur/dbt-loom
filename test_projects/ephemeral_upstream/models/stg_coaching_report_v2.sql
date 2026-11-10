select
    cast(report_id as varchar) as report_id
    , tenant
    , cast(edw_dt_load as date) as edw_dt_load
    , source_system
from {{ ref('raw_coaching_reports') }}
