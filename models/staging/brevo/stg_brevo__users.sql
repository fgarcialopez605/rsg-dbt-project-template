WITH

src_data AS (
    SELECT
        EMAIL as EMAIL -- TEXT
        , IS_OWNER as IS_OWNER -- BOOLEAN
        , STATUS as STATUS -- TEXT
        , FEATURE_ACCESS as FEATURE_ACCESS -- TEXT
        , ID as ID -- TEXT
        
    FROM {{ source("BREVO", "USERS")}}
)

SELECT * FROM src_data