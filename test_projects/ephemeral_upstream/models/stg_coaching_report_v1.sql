select
    report_id
    , tenant
    , edw_dt_load
from {{ ref('raw_coaching_reports') }}
