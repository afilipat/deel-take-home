SELECT
    external_ref,
    date_time,
    state,
    amount_usd,
    country,
    currency
FROM {{ ref('payment_acceptance') }}
WHERE chargeback_processed IS NULL
-- Ordering by date helps identify if the missing data is due to recent transactions
ORDER BY date_time DESC