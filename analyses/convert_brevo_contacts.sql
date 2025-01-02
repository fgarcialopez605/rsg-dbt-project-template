WITH

src_data AS (
    SELECT
        ID as ID -- NUMBER
        , EMAILBLACKLISTED as EMAILBLACKLISTED -- BOOLEAN
        , SMSBLACKLISTED as SMSBLACKLISTED -- BOOLEAN
        , CREATEDAT as CREATEDAT -- TEXT
        , MODIFIEDAT as MODIFIEDAT -- TEXT
        , EMAIL as EMAIL -- TEXT
        , LISTIDS as LISTIDS -- TEXT
        , LISTUNSUBSCRIBED as LISTUNSUBSCRIBED -- NUMBER
        , PARSE_JSON(ATTRIBUTES) AS ATTRIBUTES -- VARIANT

    FROM {{ source("BREVO", "CONTACTS")}}
),
flattened_data AS (
    SELECT
        ID as ID -- NUMBER
        , EMAILBLACKLISTED as EMAILBLACKLISTED
        , SMSBLACKLISTED as SMSBLACKLISTED
        , TO_TIMESTAMP_TZ(CREATEDAT) AS CREATEDAT
        , TO_TIMESTAMP_TZ(MODIFIEDAT) AS MODIFIEDAT
        , CAST(PARSE_JSON(LISTIDS) AS ARRAY(NUMBER)) AS LISTIDS
        , LISTUNSUBSCRIBED as LISTUNSUBSCRIBED
        , ATTRIBUTES
        , ATTRIBUTES:FIRSTNAME::STRING AS ATTRIBUTES_FIRSTNAME
        , ATTRIBUTES:LANGUAGE::STRING AS ATTRIBUTES_LANGUAGE
        , ATTRIBUTES:LASTNAME::STRING AS ATTRIBUTES_LASTNAME
        , ATTRIBUTES:PAGEPATH::STRING AS ATTRIBUTES_PAGEPATH
    FROM src_data
)

SELECT * FROM flattened_data