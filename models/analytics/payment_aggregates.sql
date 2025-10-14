SELECT
    ar.external_ref,
    ar.status,
    ar.source,
    ar.ref,
    ar.date_time,
    ar.state,
    ar.cvv_provided,
    ar.amount,
    ar.country,
    ar.currency,
    ar.rates,
    cr.reason AS chargeback_reason,
    CASE WHEN cr.external_ref IS NULL THEN TRUE ELSE FALSE END AS accepted
FROM {{ ref('staging_acceptance') }} AS ar
LEFT JOIN {{ ref('staging_chargeback') }} AS cr ON cr.external_ref = ar.external_ref