select
    report_id
    , tenant
    , edw_dt_load
from {{ ref('raw_coach_the_coach') }}
