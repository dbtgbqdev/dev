{{ config(
    materialized='incremental',
    unique_key=['JournalEntryHeaderAeHeaderId', 'JournalEntryHeaderApplicationId']
) }}

SELECT * FROM `osjl-kt.prestage.fscmtopmodelam_finextractam_xlabiccextractam_subledgerjournalheaderextractpvo`
--{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  -- (uses >= to include records whose timestamp occurred since the last run of this model)
  -- (If event_time is NULL or the table is truncated, the condition will always be true and load all records)
  --WHERE LASTUPDATEDATE >= (SELECT MAX(LASTUPDATEDATE) FROM {{ this }})
--{% endif %}