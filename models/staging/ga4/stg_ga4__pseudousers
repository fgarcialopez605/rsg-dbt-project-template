{{
  config(
    materialized = "table",
    table_format="iceberg",
    external_volume="ga4_dbt_iceberg_external_volume",
  )
}}
with source as (
     SELECT * FROM {{ source('GA4', 'PSEUDONYMOUS_USERS_410564878') }}
),
transformed as (
    SELECT
        RUN_ID,
        SOURCE_TABLE_DATE,
        EXTRACT(YEAR FROM SOURCE_TABLE_DATE) AS year,
        EXTRACT(MONTH FROM SOURCE_TABLE_DATE) AS month,
        EXTRACT(DAY FROM SOURCE_TABLE_DATE) AS day,
        INGESTION_COMPLETE
    FROM source
    WHERE INGESTION_COMPLETE = TRUE
)
SELECT * FROM transformed