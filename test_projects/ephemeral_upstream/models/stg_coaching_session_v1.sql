select
    report_id || '||' || tenant as bk_coaching_session
    , edw_dt_load
from {{ ref('stg_coaching_report', v=1) }}
