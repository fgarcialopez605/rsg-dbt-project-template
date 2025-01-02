WITH parsed_data AS (
    SELECT DISTINCT
        ORGANIZATION_ID as ORGANIZATION_ID -- TEXT
        , USER_ID as USER_ID -- NUMBER
        , PARSE_JSON(PLAN) AS PLAN
        , PARSE_JSON(PLANVERTICALS) AS PLANVERTICALS
    FROM TEST.BREVO.ACCOUNTS
),
flattened_data AS (
    SELECT
        ORGANIZATION_ID
        , USER_ID
        , d.value AS PLAN
        , d.value:credits::NUMBER AS PLAN_CREDITS
        , d.value:creditsType::STRING AS PLAN_CREDITSTYPE
        , d.value:type::STRING AS PLAN_TYPE
        , TO_DATE(d.value:startDate) AS PLAN_STARTDATE
        , TO_DATE(d.value:endDate) AS PLAN_ENDDATE
        , t.value AS PLANVERTICALS
        , t.value:credits::STRING AS PLANVERTICALS_CREDITS
        , t.value:name::STRING AS PLANVERTICALS_NAME
        , t.value:planCategory::STRING AS PLANVERTICALS_PLANCATEGORY
        , t.value:planType::STRING AS PLANVERTICALS_PLANTYPE
        , t.value:status::STRING AS PLANVERTICALS_STATUS
        , TO_TIMESTAMP(t.value:startDate) AS PLANVERTICALS_STARTDATE
        , TO_TIMESTAMP(t.value:endDate) AS PLANVERTICALS_ENDDATE
        , t.value:users:purchasedSeats::NUMBER AS PLANVERTICALS_USERS_PURCHASEDSEATS
        , t.value:users:usedSeats::NUMBER AS PLANVERTICALS_USERS_USEDSEATS
    FROM parsed_data,
    LATERAL FLATTEN(input => PLAN) d,
    LATERAL FLATTEN(input => PLANVERTICALS) t
)

SELECT *
FROM flattened_data