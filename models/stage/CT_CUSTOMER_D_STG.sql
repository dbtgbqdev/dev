{{ config(materialized='incremental',unique_key = 'INTEGRATION_ID')}}
{% set last_load_date = get_last_load_date()%}
WITH P as (select * from raw.fscmtopmodelam_partiesanalyticsam_customer),
A as (select * from raw.fscmtopmodelam_partiesanalyticsam_customeraccount),
S as (select * from raw.fscmtopmodelam_partiesanalyticsam_custacctsiteuseloc),
CT_CUSTOMER_D_STG as (
SELECT
  A.ACCOUNTNAME AS CUSTOMER_NAME,
  P.PARTYNUMBER AS PARTY_NUM,
  P.PARTYNAME AS PARTY_NAME,
  P.PARTYTYPE AS PARTY_TYPE_NAME,
  A.CUSTOMERTYPE AS ACCOUNT_TYPE_CODE,
  CASE
    WHEN A.CUSTOMERTYPE = 'R' THEN 'External'
    WHEN A.CUSTOMERTYPE = 'I' THEN 'Internal'
    ELSE A.CUSTOMERTYPE
  END AS ACCOUNT_TYPE_NAME,
  S.PARTYSITEPARTYSITENUMBER AS SITE_NUM,
  S.PARTYSITEPARTYSITENAME AS SITE_NAME,
  S.PARTYSITESTATUS AS SITE_STATUS,
  S.LOCATIONADDRESS1 AS ADDRESS1,
  S.LOCATIONADDRESS2 AS ADDRESS2,
  S.LOCATIONADDRESS3 AS ADDRESS3,
  S.LOCATIONADDRESS4 AS ADDRESS4,
  S.LOCATIONCOUNTY AS COUNTY,
  S.LOCATIONCITY AS CITY,
  S.LOCATIONSTATE AS STATE_CODE,
  S.LOCATIONSTATE AS STATE_NAME,
  S.LOCATIONPROVINCE AS STATE_REGION,
  S.LOCATIONCOUNTRY AS COUNTRY_CODE,
  S.LOCATIONCOUNTRY AS COUNTRY_NAME,
  S.LOCATIONPROVINCE AS COUNTRY_REGION,
  P.PRIMARYPHONENUMBER AS PHONE_NUM,
  'NULL' AS FAX_NUM,
  P.EMAILADDRESS AS EMAIL_ADDRESS,
  'NULL' AS WEB_ADDRESS,
  S.LOCATIONPOSTALCODE AS POSTAL_CODE,
  'NULL' AS ADDR_LATITUDE,
  'NULL' AS ADDR_LONGITUDE,
  P.PERSONLASTNAME AS PRMRY_CNTCT_NAME,
  A.CUSTOMERCLASSCODE AS ACCOUNT_CLASS_CODE,
  'NULL' AS ACCOUNT_CLASS_NAME,
  'NULL' AS FRGHT_TERMS_CODE,
  'NULL' AS FRGHT_TERMS_NAME,
  CAST(A.ACCOUNTTERMINATIONDATE AS DATE) AS ACCOUNT_TERMINATION_DT,
  CAST(A.ACCOUNTESTABLISHEDDATE AS DATE) AS ACCOUNT_SINCE_DT,
  'NULL' AS ALT_ACCOUNT_NUM,
  A.ACCOUNTNUMBER AS ACCOUNT_NUM,
  'NULL' AS COMPANY_CODE,
  'NULL' AS COMPANY_NAME,
  'NULL' AS PAY_TERMS_CODE,
  'NULL' AS PAY_TERMS_NAME,
  CASE
    WHEN A.STATUS = 'A' THEN 'Active'
    WHEN A.STATUS = 'I' THEN 'Inactive'
    ELSE A.STATUS
  END AS ACCOUNT_STATUS,
  A.ACCOUNTNAME AS ACCOUNT_DESCRIPTION,
  A.CUSTOMERCLASSCODE AS CLASSIFICATION,
  'NULL' AS PARTY_RELATIONSHIP_FLAG,
  'NULL' AS ACCOUNT_RELATIONSHIP_FLAG,
  'NULL' AS ATTRIBUTE1,
  'NULL' AS ATTRIBUTE2,
  'NULL' AS ATTRIBUTE3,
  'NULL' AS ATTRIBUTE4,
  'NULL' AS ATTRIBUTE5,
  S.CUSTOMERACCOUNTSITEUSECREATIONDATE AS CREATION_DT,
  S.CUSTOMERACCOUNTSITEUSELASTUPDATEDATE AS LAST_UPDATE_DT,
  S.CUSTOMERACCOUNTSITEUSECREATEDBY AS CREATED_BY,
  S.CUSTOMERACCOUNTSITEUSELASTUPDATEDBY AS LAST_UPDATED_BY,
  CONCAT(CAST(S.CUSTOMERACCOUNTSITEUSESITEUSEID AS STRING), '~',
         CAST(S.CUSTOMERACCOUNTSITECUSTACCTSITEID AS STRING), '~',
         CAST(S.CUSTACCOUNTID AS STRING)) AS INTEGRATION_ID,
  1000 AS DATASOURCE_NUM_ID,
  'Y' AS CURRENT_FLAG,
  'N' AS DELETE_FLAG,
  CURRENT_TIMESTAMP() AS W_INSERT_DT,
  CURRENT_TIMESTAMP() AS W_UPDATE_DT,
  'NULL' AS PROSPECT_FLG,
  P.DUNSNUMBERC AS DUNS_NUM,
  CASE
    WHEN P.PARTYTYPE = 'ORGANIZATION' THEN P.PARTYID
    ELSE 'NULL'
  END AS SUPPLIER_ID,
  A.ACCOUNTNUMBER AS CUSTOMER_NUM,
  CASE
    WHEN P.PARTYTYPE = 'ORGANIZATION' THEN P.PARTYNUMBER
    ELSE 'NULL'
  END AS VENDOR_NUM,
  CASE
    WHEN P.PARTYTYPE = 'ORGANIZATION' THEN P.PARTYNAME
    ELSE 'NULL'
  END AS VENDOR_NAME,
  'N' AS CUSTOMER_VENDOR_INDICATOR,
  P.PARTYID AS PARTY_ID,
  CASE
    WHEN P.PARTYTYPE = 'ORGANIZATION' THEN P.PARTYID
    ELSE 'NULL'
  END AS PARTY_ORG_ID,
  CASE
    WHEN P.PARTYTYPE = 'PERSON' THEN P.PARTYID
    ELSE 'NULL'
  END AS PARTY_PER_ID,
  P.STATUS AS PARTY_STATUS,
  'N' AS CREDIT_CHECK_FLAG,
  A.HOLDBILLFLAG AS CREDIT_HOLD_FLAG,
  'NULL' AS SITE_USE_CODE,
  'NULL' AS INVOICE_DELIVERY_METHOD  
FROM
   P,
   A,
   S
WHERE
  P.PARTYID = A.PARTYID
  AND A.CUSTACCOUNTID = S.CUSTACCOUNTID
  AND S.CUSTOMERACCOUNTSITEUSESTATUS = 'A'
 AND CAST(LAST_UPDATE_DT  AS STRING FORMAT 'YYYY-MM-DD')>= CAST( {{last_load_date}}  as STRING)
)
SELECT * FROM CT_CUSTOMER_D_STG