WITH

src_data AS (
    SELECT
        COUNT as COUNT -- NUMBER
        , PARSE_JSON(TEMPLATES) AS TEMPLATES
        
    FROM {{ source("BREVO", "SMTP_TEMPLATES")}}
),

flattened_data AS (
    SELECT
        COUNT,
        d.value AS TEMPLATES
        , TO_TIMESTAMP_TZ(d.value:createdAt) AS TEMPLATES_CREATEDAT
        , d.value:htmlContent::STRING AS TEMPLATES_HTMLCONTENT
        , d.value:id::NUMBER AS TEMPLATES_ID
        , d.value:isActive::BOOLEAN AS TEMPLATES_ISACTIVE
        , TO_TIMESTAMP_TZ(d.value:modifiedAt) AS TEMPLATES_MODIFIEDAT
        , d.value:name::STRING AS TEMPLATES_NAME
        , d.value:replyTo::STRING AS TEMPLATES_REPLYTO
        , d.value:sender:email::STRING AS TEMPLATES_SENDER_EMAIL
        , d.value:sender:id::NUMBER AS TEMPLATES_SENDER_ID
        , d.value:sender:name::STRING AS TEMPLATES_SENDER_NAME
        , d.value:subject::STRING AS TEMPLATES_SUBJECT
        , d.value:tag::STRING AS TEMPLATES_TAG
        , d.value:testSent::BOOLEAN AS TEMPLATES_TESTSENT
        , d.value:toField::STRING AS TEMPLATES_TOFIELD
        
    FROM src_data,
    LATERAL FLATTEN(input => TEMPLATES) d
    
)

SELECT * FROM flattened_data