WITH

src_data AS (
    SELECT DISTINCT
        PARSE_JSON(DOMAINS) AS DOMAINS
        , COUNT as COUNT -- NUMBER
        , CURRENT_PAGE as CURRENT_PAGE -- NUMBER
        , TOTAL_PAGES as TOTAL_PAGES -- NUMBER

    FROM {{ source("BREVO", "DOMAINS") }}
),

flattened_data AS (
    SELECT
        d.value AS DOMAINS
        , d.value:authenticated::BOOLEAN AS DOMAINS_AUTHENTICATED
        , TO_TIMESTAMP_TZ(d.value:authenticator:creationDate) AS DOMAINS_AUTHENTICATOR_CREATIONDATE
        , d.value:authenticator:email::STRING AS DOMAINS_AUTHENTICATOR_EMAIL
        , d.value:authenticator:id::STRING AS DOMAINS_AUTHENTICATOR_ID
        , d.value:authenticator:method::STRING AS DOMAINS_AUTHENTICATOR_METHOD
        , TO_TIMESTAMP_TZ(d.value:creator:creationDate) AS DOMAINS_CREATOR_CREATIONDATE
        , d.value:creator:email::STRING AS DOMAINS_CREATOR_EMAIL
        , d.value:creator:id::STRING AS DOMAINS_CREATOR_ID
        , d.value:dc_status::STRING AS DOMAINS_DC_STATUS
        , d.value:domain_name::STRING AS DOMAINS_NAME
        , d.value:id::STRING AS DOMAINS_ID
        , d.value:provider::STRING AS DOMAINS_PROVIDER
        , d.value:verified::BOOLEAN AS DOMAINS_VERIFIED
        , COUNT AS COUNT -- NUMBER
        , CURRENT_PAGE AS CURRENT_PAGE -- NUMBER
        , TOTAL_PAGES AS TOTAL_PAGES -- NUMBER
    FROM src_data,
    LATERAL FLATTEN(input => DOMAINS) d
    
)

SELECT * FROM flattened_data