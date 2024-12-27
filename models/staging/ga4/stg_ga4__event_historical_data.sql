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
    CAST("collected_traffic_source" AS MAP(VARCHAR, VARCHAR)) AS "collected_traffic_source",
    TRY_CAST("device_mobile_brand_name"::STRING AS STRING) AS "device_mobile_brand_name",
    TRY_CAST("device_mobile_marketing_name"::STRING AS STRING) AS "device_mobile_marketing_name",
    TRY_CAST("device_mobile_model_name"::STRING AS STRING) AS "device_mobile_model_name",
    TRY_CAST("device_mobile_os_hardware_model"::STRING AS STRING) AS "device_mobile_os_hardware_model",
    TRY_CAST("device_web_info_browser_version"::STRING AS STRING) AS "device_web_info_browser_version",
    CAST("items"::VARIANT AS ARRAY(NUMBER)) AS "items",
    CAST("session_traffic_source_last_click" AS MAP(VARCHAR, MAP(VARCHAR, VARCHAR))) AS "session_traffic_source_last_click",
    TRY_CAST("user_ltv_currency"::STRING AS STRING) AS "user_ltv_currency",
    CAST("user_properties"::VARIANT AS ARRAY(NUMBER)) AS "user_properties",
    TO_DATE("event_date", 'YYYYMMDD') AS "event_date",
    TO_TIMESTAMP("event_timestamp" / 1000000) AS "event_timestamp",
    TO_TIMESTAMP("user_first_touch_timestamp" / 1000000) AS "user_first_touch_timestamp"
    FROM source
)
SELECT * FROM transformed