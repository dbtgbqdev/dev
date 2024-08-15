{{ config(
    materialized='table',
    full_refresh=true
) }}

{{ insert_unspecified_record('osjl-kt.stage.CT_CUSTOMER_D_STG') }}
