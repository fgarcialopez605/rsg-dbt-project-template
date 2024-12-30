WITH

src_data as (
    SELECT
        ORGANIZATION_ID as ORGANIZATION_ID -- TEXT
        , USER_ID as USER_ID -- NUMBER
        , ENTERPRISE as ENTERPRISE -- BOOLEAN
        , EMAIL as EMAIL -- TEXT
        , FIRSTNAME as FIRSTNAME -- TEXT
        , LASTNAME as LASTNAME -- TEXT
        , COMPANYNAME as COMPANYNAME -- TEXT
        , PLAN as PLAN -- TEXT
        , PLANVERTICALS as PLANVERTICALS -- TEXT
        , RELAY_ENABLED as RELAY_ENABLED -- BOOLEAN
        , RELAY_DATA_USERNAME as RELAY_DATA_USERNAME -- TEXT
        , RELAY_DATA_RELAY as RELAY_DATA_RELAY -- TEXT
        , RELAY_DATA_PORT as RELAY_DATA_PORT -- NUMBER
        , MARKETINGAUTOMATION_KEY as MARKETINGAUTOMATION_KEY -- TEXT
        , MARKETINGAUTOMATION_ENABLED as MARKETINGAUTOMATION_ENABLED -- BOOLEAN
        , ADDRESS_CITY as ADDRESS_CITY -- TEXT
        , ADDRESS_STREET as ADDRESS_STREET -- TEXT
        , ADDRESS_ZIPCODE as ADDRESS_ZIPCODE -- TEXT
        , ADDRESS_COUNTRY as ADDRESS_COUNTRY -- TEXT
    FROM {{ source("BREVO", "ACCOUNTS")}}
)

SELECT * FROM src_data