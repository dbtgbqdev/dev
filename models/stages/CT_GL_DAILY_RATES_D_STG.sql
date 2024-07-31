WITH finextractam_gl as (select * from raw.fscmtopmodelam_finextractam_glbiccextractam_dailyrateextractpvo),
CT_GL_DAILY_RATES_D_STG as (
select
  A.FROM_CURRENCY AS FROM_CURRENCY,
  A.TO_CURRENCY AS TO_CURRENCY,
  A.CONVERSION_DT AS CONVERSION_DT,
  A.CONVERSION_TYPE AS CONVERSION_TYPE,
  A.CONVERSION_RATE AS CONVERSION_RATE,
  A.STATUS_CODE AS STATUS_CODE,
  A.CREATION_DT AS CREATION_DT,
  A.CREATED_BY AS CREATED_BY,
  A.LAST_UPDATE_DT AS LAST_UPDATE_DT,
  A.LAST_UPDATED_BY AS LAST_UPDATED_BY,
  A.LAST_UPDATE_LOGIN AS LAST_UPDATE_LOGIN,
  A.CONTEXT AS CONTEXT,
  A.ATTRIBUTE1 AS ATTRIBUTE1,
  A.ATTRIBUTE2 AS ATTRIBUTE2,
  A.ATTRIBUTE3 AS ATTRIBUTE3,
  A.ATTRIBUTE4 AS ATTRIBUTE4,
  A.ATTRIBUTE5 AS ATTRIBUTE5,
  A.ATTRIBUTE6 AS ATTRIBUTE6,
  A.ATTRIBUTE7 AS ATTRIBUTE7,
  A.ATTRIBUTE8 AS ATTRIBUTE8,
  A.ATTRIBUTE9 AS ATTRIBUTE9,
  A.ATTRIBUTE10 AS ATTRIBUTE10,
  A.ATTRIBUTE11 AS ATTRIBUTE11,
  A.ATTRIBUTE12 AS ATTRIBUTE12,
  A.ATTRIBUTE13 AS ATTRIBUTE13,
  A.ATTRIBUTE14 AS ATTRIBUTE14,
  A.ATTRIBUTE15 AS ATTRIBUTE15,
  A.RATE_SOURCE_CODE AS RATE_SOURCE_CODE,
  A.OBJECT_VERSION_NUMBER AS OBJECT_VERSION_NUMBER,
  A.ENTERPRISE_ID AS ENTERPRISE_ID,
  A.INTEGRATION_ID AS INTEGRATION_ID,
  A.DATASOURCE_NUM_ID AS DATASOURCE_NUM_ID,
  A.DESCRIPTION AS DESCRIPTION,
  'Y' AS ACTIVE_FLAG,
  'N' AS DELETE_FLAG,
  CURRENT_TIMESTAMP() AS W_INSERT_DT,
  CURRENT_TIMESTAMP() AS W_UPDATE_DT
FROM
  (
    SELECT DISTINCT
      DAILY.DAILYRATEFROMCURRENCY AS FROM_CURRENCY,
      DAILY.DAILYRATETOCURRENCY AS TO_CURRENCY,
      DAILY.DAILYRATECONVERSIONDATE AS CONVERSION_DT,
      DAILY.DAILYRATECONVERSIONTYPE AS CONVERSION_TYPE,
      CAST(DAILY.DAILYRATECONVERSIONRATE AS FLOAT64) AS CONVERSION_RATE,
      DAILY.DAILYRATESTATUSCODE AS STATUS_CODE,
      CAST(DAILY.DAILYRATECREATIONDATE AS TIMESTAMP) AS CREATION_DT,
      DAILY.DAILYRATECREATEDBY AS CREATED_BY,
      CAST(DAILY.DAILYRATELASTUPDATEDATE AS TIMESTAMP) AS LAST_UPDATE_DT,
      DAILY.DAILYRATELASTUPDATEDBY AS LAST_UPDATED_BY,
      DAILY.DAILYRATELASTUPDATELOGIN AS LAST_UPDATE_LOGIN,
      'GL' AS CONTEXT,
      'NULL' AS ATTRIBUTE1,
      'NULL' AS ATTRIBUTE2,
      'NULL' AS ATTRIBUTE3,
      'NULL' AS ATTRIBUTE4,
      'NULL' AS ATTRIBUTE5,
      'NULL' AS ATTRIBUTE6,
      'NULL' AS ATTRIBUTE7,
      'NULL' AS ATTRIBUTE8,
      'NULL' AS ATTRIBUTE9,
      'NULL' AS ATTRIBUTE10,
      'NULL' AS ATTRIBUTE11,
      'NULL' AS ATTRIBUTE12,
      'NULL' AS ATTRIBUTE13,
      'NULL' AS ATTRIBUTE14,
      'NULL' AS ATTRIBUTE15,
      'NULL' AS RATE_SOURCE_CODE,
      CAST(DAILY.DAILYRATEOBJECTVERSIONNUMBER AS INT64) AS OBJECT_VERSION_NUMBER,
      'NULL' AS ENTERPRISE_ID,
      CONCAT(
        DAILY.DAILYRATEFROMCURRENCY, '~',
        DAILY.DAILYRATETOCURRENCY, '~',
        --FORMAT_TIMESTAMP('%F', DAILY.DAILYRATECONVERSIONDATE), '~',
        DAILY.DAILYRATECONVERSIONDATE,
        DAILY.DAILYRATECONVERSIONTYPE
      ) AS INTEGRATION_ID,
      1000 AS DATASOURCE_NUM_ID,
      'NULL' AS DESCRIPTION
    FROM
      finextractam_gl as DAILY
      ) as A
)
SELECT * FROM CT_GL_DAILY_RATES_D_STG