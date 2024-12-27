with source as (
     SELECT * FROM {{ source('SPRINKLR', 'CAMPAIGN') }}
)
SELECT * FROM source