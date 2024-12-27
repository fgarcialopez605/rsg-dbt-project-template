{{
  config(
    materialized = "table",
    table_format="iceberg",
    external_volume="ga4_dbt_iceberg_external_volume",
  )
}}
with source as (
     SELECT * FROM {{ source('GA4_LANDING', 'PSEUDONYMOUS_USERS_HISTORICAL_DATA') }}
),
transformed as (
    SELECT
    {% for column in adapter.get_columns_in_relation(source('GA4_LANDING', 'PSEUDONYMOUS_USERS_HISTORICAL_DATA')) 
       if column.name not in ['occurrence_date', 'last_updated_date', 'last_active_timestamp_micros', 'user_first_touch_timestamp_micros', 
            'audience_start', 'audience_expiry'] 
        and column.data_type != 'VARIANT' %}
            "{{ column.name }}"{% if not loop.last %}, {% endif %}
    {% endfor %},
    TRY_CAST("mobile_model_name"::STRING AS STRING) AS "mobile_model_name",
    TRY_CAST("unified_screen_name"::STRING AS STRING) AS "unified_screen_name",
    CAST("user_properties"::VARIANT AS ARRAY(NUMBER)) AS "user_properties",
    TO_TIMESTAMP("last_active_timestamp_micros" / 1000000) AS "last_active_timestamp_micros",
    TO_TIMESTAMP("user_first_touch_timestamp_micros" / 1000000) AS "user_first_touch_timestamp_micros",
    TO_TIMESTAMP("audience_start" / 1000000) AS "audience_start",
    TO_TIMESTAMP("audience_expiry" / 1000000) AS "audience_expiry",
    TO_DATE("occurrence_date", 'YYYYMMDD') AS "occurrence_date",
    TO_DATE("last_updated_date", 'YYYYMMDD') AS "last_updated_date"
    FROM source
)
SELECT * FROM transformed