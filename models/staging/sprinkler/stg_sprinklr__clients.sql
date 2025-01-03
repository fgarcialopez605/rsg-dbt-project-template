with src_data as (
     SELECT
        CLIENTID as CLIENTID -- NUMBER
        , PARTNERID as PARTNERID -- NUMBER
        , CLIENTNAME as CLIENTNAME -- TEXT
     FROM {{ source('SPRINKLR', 'CLIENTS') }}
)

SELECT * FROM src_data