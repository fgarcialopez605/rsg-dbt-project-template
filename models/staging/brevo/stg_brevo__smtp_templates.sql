WITH

src_data AS (
    SELECT
        COUNT as COUNT -- NUMBER
        , TEMPLATES as TEMPLATES -- TEXT
        
    FROM {{ source("BREVO", "SMTP_TEMPLATES")}}
)

SELECT * FROM src_data