with detections as (
    select
        cast(message_id as text) as message_id,
        detected_object_class,
        confidence_score
    from {{ source('raw', 'image_detections') }}
),
messages as (
    select message_id from {{ ref('fct_messages') }}
)
select
    m.message_id,
    d.detected_object_class,
    d.confidence_score
from detections d
join messages m on d.message_id = m.message_id 