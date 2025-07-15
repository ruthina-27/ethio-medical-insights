with base as (
    select
        message_id,
        cast(message_date as date) as date,
        channel,
        message_text,
        has_image
    from {{ ref('stg_telegram_messages') }}
)
select
    b.message_id,
    d.date as date_id,
    c.channel_name as channel_id,
    b.message_text,
    b.has_image
from base b
left join {{ ref('dim_dates') }} d on b.date = d.date
left join {{ ref('dim_channels') }} c on b.channel = c.channel_name 