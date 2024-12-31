WITH parsed_data AS (
    SELECT
        ORGANIZATION_ID as ORGANIZATION_ID -- TEXT
        , USER_ID as USER_ID -- NUMBER
        , PARSE_JSON(PLAN) AS PLAN_JSON
    FROM TEST.BREVO.ACCOUNTS
),
flattened_data AS (
    SELECT
        ORGANIZATION_ID
        , USER_ID
        , PLAN_JSON
        , value AS PLAN
    FROM parsed_data,
         LATERAL FLATTEN(input => PLAN_JSON)
)
SELECT * FROM flattened_data