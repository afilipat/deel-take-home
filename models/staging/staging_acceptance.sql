SELECT
    external_ref, 
    status,
    source,
    ref,
    CAST(date_time AS TIMESTAMP) AS date_time,
    state,
    cvv_provided,
    CAST(amount AS NUMERIC) AS local_curr_amount,
    UPPER(country) AS country,
    UPPER(currency) AS currency,
    rates,
    -- USD Conversion Logic
    -- 1. Extract the exchange rate for the transaction's currency from the 'rates' JSON string.
    -- 2. Cast the extracted rate (which is a string) to NUMERIC.
    -- 3. Divide the original 'amount' by the extracted rate.
    -- 4. COALESCE handles cases where the conversion fails (e.g., rate is missing or 0).
    ROUND(
     COALESCE(
        CAST(amount AS NUMERIC) / 
        NULLIF(
            -- Use CASE to select the rate based on the currency code. Needs to be improved to have this column be a json and not string
            CASE UPPER(currency)
                WHEN 'CAD' THEN SAFE_CAST(JSON_VALUE(rates, '$.CAD') AS NUMERIC)
                WHEN 'EUR' THEN SAFE_CAST(JSON_VALUE(rates, '$.EUR') AS NUMERIC)
                WHEN 'MXN' THEN SAFE_CAST(JSON_VALUE(rates, '$.MXN') AS NUMERIC)
                WHEN 'SGD' THEN SAFE_CAST(JSON_VALUE(rates, '$.SGD') AS NUMERIC)
                WHEN 'AUD' THEN SAFE_CAST(JSON_VALUE(rates, '$.AUD') AS NUMERIC)
                WHEN 'GBP' THEN SAFE_CAST(JSON_VALUE(rates, '$.GBP') AS NUMERIC)
                WHEN 'USD' THEN 1.0 
                ELSE NULL
            END,
            0
        ),
        NULL
    )
    ,2) AS amount_usd
FROM {{ source('payment_data', 'acceptance_report')}}