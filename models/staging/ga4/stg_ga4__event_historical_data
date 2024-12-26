{{
  config(
    materialized = "table",
    table_format="iceberg",
    external_volume="ga4_dbt_iceberg_external_volume",
  )
}}
with source as (
     SELECT * FROM {{ source('GA4_LANDING', 'EVENT_HISTORICAL_DATA') }}
),
transformed as (
    SELECT
    {% for column in adapter.get_columns_in_relation(source('GA4_LANDING', 'EVENT_HISTORICAL_DATA')) 
       if column.name not in ['event_date', 'event_timestamp', 'user_first_touch_timestamp'] and column.data_type != 'VARIANT' %}
        "{{ column.name }}"{% if not loop.last %}, {% endif %}
    {% endfor %},
    TO_DATE("event_date", 'YYYYMMDD') AS "event_date",
    TO_TIMESTAMP("event_timestamp" / 1000000) AS "event_timestamp",
    TO_TIMESTAMP("user_first_touch_timestamp" / 1000000) AS "user_first_touch_timestamp"
    FROM source
)
SELECT * FROM transformed