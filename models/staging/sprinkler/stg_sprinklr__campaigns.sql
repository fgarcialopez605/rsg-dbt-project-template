with src_data as (
     SELECT
        ID AS ID -- TEXT
        , NAME as NAME -- TEXT
        , TO_TIMESTAMP(CREATEDTIME / 1000) AS CREATEDTIME -- TIMESTAMP
        , TO_TIMESTAMP(MODIFIEDTIME / 1000) AS MODIFIEDTIME -- TIMESTAMP
        , TO_TIMESTAMP(CAST(STARTDATE AS NUMBER) / 1000) AS STARTDATE -- TIMESTAMP
        , TO_TIMESTAMP(CAST(ENDDATE AS NUMBER) / 1000) AS ENDDATE -- TIMESTAMP
        , OWNER AS OWNER -- NUMBER
        , PARSE_JSON(PARTNERCUSTOMPROPERTIES) AS PARTNERCUSTOMPROPERTIES -- VARIANT
        , STATUS AS STATUS -- TEXT
        , PARSE_JSON(VISIBILITY) AS VISIBILITY -- VARIANT
        , ARCHIVED AS ARCHIVED -- BOOLEAN
        , DELETED AS DELETED -- BOOLEAN
        , DESCRIPTION AS DESCRIPTION -- TEXT 
     
     FROM {{ source('SPRINKLR', 'CAMPAIGN') }}
),

flattened_data AS (
    SELECT
        ID
        , NAME
        , CREATEDTIME
        , MODIFIEDTIME
        , STARTDATE
        , ENDDATE
        , OWNER
        , PARTNERCUSTOMPROPERTIES
        , PARTNERCUSTOMPROPERTIES:spr_campaign_goal[0]::STRING AS PARTNERCUSTOMPROPERTIES_SPR_CAMPAIGN_GOAL
        , STATUS
        , VISIBILITY:globallyVisible::BOOLEAN AS VISIBILITY_GLOBALLYVISIBLE
        , v.value AS VISIBILITY_CONFIG
        , d.value::STRING AS VISIBILITY_CONFIG_ID
        , v.value:type::STRING AS VISIBILITY_CONFIG_TYPE
        , ARCHIVED
        , DELETED
        , DESCRIPTION
    FROM src_data,
    LATERAL FLATTEN(input => VISIBILITY:visibilityConfig) v,
    LATERAL FLATTEN(input => v.value:ids) d
)

SELECT * FROM flattened_data