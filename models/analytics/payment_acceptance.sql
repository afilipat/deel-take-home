SELECT
    ar.external_ref,
    ar.status,
    ar.source,
    ar.ref,
    ar.date_time,
    ar.state,
    ar.cvv_provided,
    ar.amount_usd,
    ar.country,
    ar.currency,
    ar.rates,
    cr.chargeback AS chargeback_processed,
    CASE WHEN cr.external_ref IS NOT NULL THEN TRUE ELSE FALSE END AS chargeback_accepted
FROM {{ ref('staging_acceptance') }} AS ar
LEFT JOIN {{ ref('staging_chargeback') }} AS cr ON cr.external_ref = ar.external_ref