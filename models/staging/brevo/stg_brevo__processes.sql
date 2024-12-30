WITH

src_data AS (
    SELECT
        ID as ID -- NUMBER
        , STATUS as STATUS -- TEXT
        , NAME as NAME -- TEXT

    FROM {{ source("BREVO", "PROCESSES")}}
)

SELECT * FROM src_data