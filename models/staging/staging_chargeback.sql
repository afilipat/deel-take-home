SELECT
    external_ref,
    status,
    source,
    chargeback
FROM {{ source('payment_data', 'chargeback_report')}}