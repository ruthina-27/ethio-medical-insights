with source as (
    select
        message_json->>'id' as message_id,
        message_json->>'date' as message_date,
        message_json->>'message' as message_text,
        channel,
        (message_json->>'has_image')::boolean as has_image
    from raw.telegram_messages
)
select * from source 