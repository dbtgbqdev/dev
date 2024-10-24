{{ config(
    materialized='table',
    full_refresh=true
) }}

SELECT * FROM  {{ ref ("fscmtopmodelam_analyticsserviceam_currenciespvo")  }}