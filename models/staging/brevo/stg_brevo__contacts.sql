WITH

src_data AS (
    SELECT
        COLUMN_NAME as COLUMN_NAME -- TEXT
        , TYPE as TYPE -- TEXT
        , NULLABLE as NULLABLE -- BOOLEAN
        , EXPRESSION as EXPRESSION -- TEXT
        , FILENAMES as FILENAMES -- TEXT
        , ORDER_ID as ORDER_ID -- NUMBER

    FROM {{ source("BREVO", "CONTACTS")}}
)

SELECT * FROM src_data