select
    error_type,
    error_detail,
    raw_value,
    message_key,
    _topic as topic,
    _partition as kafka_partition,
    _offset as kafka_offset,
    timezone('UTC', _message_ts) as arrived_at,
    timezone('UTC', _ingested_at) as ingested_at,
    _batch_id as batch_id
from {{ source('bronze', 'order_events_dlq') }}
