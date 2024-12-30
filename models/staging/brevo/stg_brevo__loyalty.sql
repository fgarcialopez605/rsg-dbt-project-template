WITH

src_data AS (
    SELECT
        ITEMS as ITEMS -- TEXT

    FROM {{ source("BREVO", "LOYALTY")}}
)

SELECT * FROM src_data