{{ config(materialized='table') }}

{{ insert_unspecified_record('CT_CUSTOMER_D_STG') }}
