select
    tenant
    , count(*) as reports
from {{ ref('stg_coaching_report', v=2) }}
group by 1
