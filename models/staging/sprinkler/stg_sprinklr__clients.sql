with source as (
     SELECT * FROM {{ source('SPRINKLR', 'CLIENTS') }}
)
SELECT * FROM source