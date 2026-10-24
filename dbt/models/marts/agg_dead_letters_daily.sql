select
    cast({{ to_local('arrived_at') }} as date) as arrival_date_local,
    error_type,
    count(*) as dead_lettered_messages
from {{ ref('stg_dead_letters') }}
group by all
