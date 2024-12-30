WITH

src_data AS (
    SELECT
        DOMAINS as DOMAINS -- TEXT
        , COUNT as COUNT -- NUMBER
        , CURRENT_PAGE as CURRENT_PAGE -- NUMBER
        , TOTAL_PAGES as TOTAL_PAGES -- NUMBER

    FROM {{ source("BREVO", "DOMAINS") }}
)

SELECT * FROM src_data