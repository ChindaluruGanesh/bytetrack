SELECT
    payment_id,
    order_id,
    amount,
    cashback,
    payment_method,
    gateway,
    status,
    payment_ts,
    (status = 'success') AS is_successful
FROM {{ ref('stg_payments') }}
