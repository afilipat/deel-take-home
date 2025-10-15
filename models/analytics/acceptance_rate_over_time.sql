WITH transaction_usd_data AS (
    -- Calculate the amount_usd for every transaction/payment
    SELECT
        date_time,
        state,
        amount_usd
    FROM {{ ref("staging_acceptance")}}
),

monthly_aggregation AS (
    -- Aggregate amount metrics by month
    SELECT
        -- Extract the month from the timestamp for grouping
        DATE_TRUNC(CAST(date_time AS DATE), MONTH) AS payment_month,
        -- Total amount
        SUM(amount_usd) AS total_amount_usd,
        -- Accepted amount
        SUM(CASE WHEN state = 'ACCEPTED' THEN amount_usd ELSE 0 END) AS accepted_amount_usd,
        -- Declined amount
        SUM(CASE WHEN state = 'DECLINED' THEN amount_usd ELSE 0 END) AS declined_amount_usd,
        -- Total Transaction Count
        COUNT(date_time) AS total_transactions,
        -- Accepted Transaction Count
        COUNTIF(state = 'ACCEPTED') AS accepted_transactions,
        -- Declined Transaction Count
        COUNTIF(state = 'DECLINED') AS declined_transactions
    FROM transaction_usd_data
    GROUP BY 1 -- Month
)

-- Calculate the Acceptance Rates
SELECT
    payment_month,
    total_amount_usd,
    accepted_amount_usd,
    declined_amount_usd,
    total_transactions,
    accepted_transactions,
    declined_transactions,
    -- Transaction Acceptance Rate
    ROUND(SAFE_DIVIDE(accepted_transactions, total_transactions) * 100,2) AS transaction_acceptance_rate,
    -- Transaction Decline Rate
    ROUND(SAFE_DIVIDE(declined_transactions, total_transactions) * 100,2) AS transaction_declined_rate
FROM monthly_aggregation
ORDER BY payment_month DESC