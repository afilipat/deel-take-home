SELECT
    country,
    declined_volume_usd,
    total_volume_usd
    
FROM {{ ref('acceptance_by_country') }}
WHERE declined_volume_usd > 25000000
ORDER BY declined_volume_usd DESC