SELECT
    country,
    SUM(amount_usd) AS total_volume_usd,
    COUNT(external_ref) AS total_transactions,
    -- Accepted Count (Count where state = 'ACCEPTED')
    COUNTIF(state = 'ACCEPTED') AS accepted_transactions,
    -- Accepted Volume (Volume where state = 'ACCEPTED')
    SUM(CASE WHEN state = 'ACCEPTED' THEN amount_usd ELSE 0 END) AS accepted_volume_usd,
    -- Declined Count (Count where state = 'DECLINED')
    COUNTIF(state = 'DECLINED') AS declined_transactions,
    -- Declined Volume (Volume where state = 'DECLINED')
    SUM(CASE WHEN state = 'DECLINED' THEN amount_usd ELSE 0 END) AS declined_volume_usd,
    -- Acceptance Rate (%) - Accepted transactions divided by total transactions.
    SAFE_DIVIDE(COUNTIF(state = 'ACCEPTED'), COUNT(external_ref)) AS acceptance_rate,
    -- Volume Acceptance Rate (%) - Accepted volume divided by total volume.
    SAFE_DIVIDE(
        SUM(CASE WHEN state = 'ACCEPTED' THEN amount_usd ELSE 0 END), 
        SUM(amount_usd)
    ) AS volume_acceptance_rate

FROM {{ ref('payment_acceptance') }}

GROUP BY 1 -- Group by country
ORDER BY total_volume_usd DESC