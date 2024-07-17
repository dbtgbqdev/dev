{{ config(
    materialized='incremental',
    unique_key='DisabilityId,EffectiveStartDate'
) }}

SELECT * FROM osjl-kt.prestage.hcmtopmodelanalyticsglobalam_personam_persondisabilitypvo
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  -- (If event_time is NULL or the table is truncated, the condition will always be true and load all records)
  WHERE LASTUPDATEDATE >= (SELECT COALESCE(MAX(LASTUPDATEDATE), '1900-01-01'::TIMESTAMP) FROM { '{' } this { '}' })
{% endif %}
