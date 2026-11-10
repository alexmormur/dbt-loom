select distinct
    tenant
from {{ source('raw', 'raw_coach_the_coach') }}
where {{ is_known_tenant('tenant') }}
