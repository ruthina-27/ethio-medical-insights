select distinct cast(message_date as date) as date
from {{ ref('stg_telegram_messages') }} 