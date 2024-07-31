WITH balanceextract AS (
    SELECT 
        BALANCEACTUALFLAG AS ACTUAL_FLAG,
        BALANCEBEGINBALANCECR AS BALANCE_CR,
        BALANCEBEGINBALANCEDR AS BALANCE_DR,
        BALANCEOBJECTVERSIONNUMBER AS BUDGET_VERSION_ID,
        BALANCECURRENCYCODE AS CURRENCY_CODE,
        BALANCEENCUMBRANCETYPEID AS ENCUMBRANCE_TYPE_ID,
        BALANCECODECOMBINATIONID AS GLCC_ID,
        BALANCELASTUPDATEDATE AS LAST_UPDATE_DT,
        BALANCELASTUPDATEDBY AS LAST_UPDATED_BY,
        BALANCELEDGERID AS LEDGER_ID,
        BALANCEPERIODNAME AS PERIOD_NAME,
        BALANCEPERIODNETCR AS PERIOD_NET_CR,
        BALANCEPERIODNETDR AS PERIOD_NET_DR,
        BALANCEPERIODNUM AS PERIOD_NUM,
        BALANCEPERIODYEAR AS PERIOD_YEAR,
        BALANCEQUARTERTODATECR AS QTR_TO_DATE_CR,
        BALANCEQUARTERTODATEDR AS QTR_TO_DATE_DR,
        BALANCETRANSLATEDFLAG AS TRANSLATED_FLAG
    FROM raw.fscmtopmodelam_finextractam_glbiccextractam_balanceextractpvo
    --SELECT * FROM raw.fscmtopmodelam_finextractam_glbiccextractam_balanceextractpvo
),
fiscalperiodextract AS (
    SELECT 
    PERIODADJUSTMENTPERIODFLAG AS ADJUSTMENT_PERIOD_FLAG,
    PERIODATTRIBUTECATEGORY AS ATTRIBUTE_CATEGORY,
    PERIODATTRIBUTE1 AS ATTRIBUTE1,
    PERIODATTRIBUTE2 AS ATTRIBUTE2,
    PERIODATTRIBUTE3 AS ATTRIBUTE3,
    PERIODATTRIBUTE4 AS ATTRIBUTE4,
    PERIODATTRIBUTE5 AS ATTRIBUTE5,
    PERIODATTRIBUTE6 AS ATTRIBUTE6,
    PERIODATTRIBUTE7 AS ATTRIBUTE7,
    PERIODATTRIBUTE8 AS ATTRIBUTE8,
    PERIODCONFIRMATIONSTATUS AS CONFIRMATION_STATUS,
    PERIODCREATEDBY AS CREATED_BY,
    PERIODCREATIONDATE AS CREATION_DT,
    PERIODDESCRIPTION AS DESCRIPTION,
    PERIODENTEREDPERIODNAME AS ENTERED_PERIOD_NAME,
    PERIODGLOBALATTRIBUTECATEGORY AS GLOBAL_ATTRIBUTE_CATEGORY,
    PERIODGLOBALATTRIBUTEDATE1 AS GLOBAL_ATTRIBUTE_DATE1,
    PERIODGLOBALATTRIBUTEDATE2 AS GLOBAL_ATTRIBUTE_DATE2,
    PERIODGLOBALATTRIBUTEDATE3 AS GLOBAL_ATTRIBUTE_DATE3,
    PERIODGLOBALATTRIBUTEDATE4 AS GLOBAL_ATTRIBUTE_DATE4,
    PERIODGLOBALATTRIBUTEDATE5 AS GLOBAL_ATTRIBUTE_DATE5,
    PERIODGLOBALATTRIBUTENUMBER1 AS GLOBAL_ATTRIBUTE_NUMBER1,
    PERIODGLOBALATTRIBUTENUMBER2 AS GLOBAL_ATTRIBUTE_NUMBER2,
    PERIODGLOBALATTRIBUTENUMBER3 AS GLOBAL_ATTRIBUTE_NUMBER3,
    PERIODGLOBALATTRIBUTENUMBER4 AS GLOBAL_ATTRIBUTE_NUMBER4,
    PERIODGLOBALATTRIBUTENUMBER5 AS GLOBAL_ATTRIBUTE_NUMBER5,
    PERIODGLOBALATTRIBUTE1 AS GLOBAL_ATTRIBUTE1,
    PERIODGLOBALATTRIBUTE10 AS GLOBAL_ATTRIBUTE10,
    PERIODGLOBALATTRIBUTE11 AS GLOBAL_ATTRIBUTE11,
    PERIODGLOBALATTRIBUTE12 AS GLOBAL_ATTRIBUTE12,
    PERIODGLOBALATTRIBUTE13 AS GLOBAL_ATTRIBUTE13,
    PERIODGLOBALATTRIBUTE14 AS GLOBAL_ATTRIBUTE14,
    PERIODGLOBALATTRIBUTE15 AS GLOBAL_ATTRIBUTE15,
    PERIODGLOBALATTRIBUTE16 AS GLOBAL_ATTRIBUTE16,
    PERIODGLOBALATTRIBUTE17 AS GLOBAL_ATTRIBUTE17,
    PERIODGLOBALATTRIBUTE18 AS GLOBAL_ATTRIBUTE18,
    PERIODGLOBALATTRIBUTE19 AS GLOBAL_ATTRIBUTE19,
    PERIODGLOBALATTRIBUTE2 AS GLOBAL_ATTRIBUTE2,
    PERIODGLOBALATTRIBUTE20 AS GLOBAL_ATTRIBUTE20,
    PERIODGLOBALATTRIBUTE3 AS GLOBAL_ATTRIBUTE3,
    PERIODGLOBALATTRIBUTE4 AS GLOBAL_ATTRIBUTE4,
    PERIODGLOBALATTRIBUTE5 AS GLOBAL_ATTRIBUTE5,
    PERIODGLOBALATTRIBUTE6 AS GLOBAL_ATTRIBUTE6,
    PERIODGLOBALATTRIBUTE7 AS GLOBAL_ATTRIBUTE7,
    PERIODGLOBALATTRIBUTE8 AS GLOBAL_ATTRIBUTE8,
    PERIODGLOBALATTRIBUTE9 AS GLOBAL_ATTRIBUTE9,
    PERIODLASTUPDATEDATE AS LAST_UPDATE_DT,
    PERIODLASTUPDATELOGIN AS LAST_UPDATE_LOGIN,
    PERIODLASTUPDATEDBY AS LAST_UPDATED_BY,
    PERIODOBJECTVERSIONNUMBER AS OBJECT_VERSION_NUMBER,
    PERIODENDDATE AS PERIOD_DT_ID,
    PERIODPERIODNAME AS PERIOD_NAME,
    PERIODPERIODNUM AS PERIOD_NUM,
    PERIODPERIODSETNAME AS PERIOD_SET_NAME,
    PERIODPERIODTYPE AS PERIOD_TYPE,
    PERIODPERIODYEAR AS YEAR_START_DT,
    PERIODQUARTERNUM AS QUARTER_NUM,
    PERIODQUARTERSTARTDATE AS QUARTER_START_DT,
    PERIODSTARTDATE AS START_DT,
    PERIODYEARSTARTDATE AS YEAR_START_DT
FROM raw.fscmtopmodelam_finextractam_glbiccextractam_fiscalperiodextractpvo
),
ledgerextractpvo AS (
    SELECT LEDGERACCOUNTEDPERIODTYPE AS ACCOUNTED_PERIOD_TYPE,
    LEDGERALLOWINTERCOMPANYPOSTFLAG AS ALLOW_INTERCOMPANY_POST_FLAG,
    LEDGERAPDOCSEQUENCINGOPTIONFLAG AS AP_DOC_SEQUENCING_OPTION_FLAG,
    LEDGERARDOCSEQUENCINGOPTIONFLAG AS AR_DOC_SEQUENCING_OPTION_FLAG,
    LEDGERAUTOREVAFTEROPENPRDFLAG AS AUTO_REV_AFTER_OPEN_PRD_FLAG,
    LEDGERAUTOMATESECJRNLREVFLAG AS AUTOMATE_SECJRNLREV_FLAG,
    LEDGERBALSEGCOLUMNNAME AS BALSEG_COLUMN_NAME,
    LEDGERBALSEGVALUEOPTIONCODE AS BALSEG_VALUE_OPTION_CODE,
    LEDGERBALSEGVALUESETID AS BALSEG_VALUE_SET_ID,
    LEDGERCHARTOFACCOUNTSID AS CHART_OF_ACCOUNTS_ID,
    LEDGERCOMPLETEFLAG AS COMPLETE_FLAG,
    LEDGERCOMPLETIONSTATUSCODE AS COMPLETION_STATUS_CODE,
    LEDGERCONFIGURATIONID AS CONFIGURATION_ID,
    LEDGERCREATEDBY AS CREATED_BY,
    LEDGERCREATIONDATE AS CREATION_DT,
    LEDGERCRITERIASETID AS CRITERIA_SET_ID,
    LEDGERCUMTRANSCODECOMBINATIONID AS CUM_TRANS_CODE_COMBINATION_ID,
    LEDGERDAILYTRANSLATIONRATETYPE AS DAILY_TRANSLATION_RATE_TYPE,
    LEDGERDESCRIPTION AS DESCRIPTION,
    LEDGERENABLEAUTOMATICTAXFLAG AS ENABLE_AUTOMATIC_TAX_FLAG,
    LEDGERENABLEAVERAGEBALANCESFLAG AS ENABLE_AVERAGE_BALANCES_FLAG,
    LEDGERENABLEBUDGETARYCONTROLFLAG AS ENABLE_BUDGETARY_CONTROL_FLAG,
    LEDGERENABLEJEAPPROVALFLAG AS ENABLE_JE_APPROVAL_FLAG,
    LEDGERENABLERECONCILIATIONFLAG AS ENABLE_RECONCILIATION_FLAG,
    LEDGERENABLEREVALSSTRACKFLAG AS ENABLE_REVALSSTR_ACK_FLAG,
    LEDGERENFSEQDATECORRELATIONCODE AS ENF_SEQ_DATE_CORRELATION_CODE,
    LEDGERFIRSTLEDGERPERIODNAME AS FIRST_LEDGER_PERIOD_NAME,
    LEDGERFUTUREENTERABLEPERIODSLIMIT AS FUTURE_ENTERABLE_PERIODS_LIMIT,
    LEDGERIMPLICITACCESSSETID AS IMPLICIT_ACCESS_SET_ID,
    LEDGERINTERCOGAINLOSSCCID AS INTERCO_GAIN_LOSS_CC_ID,
    LEDGERJRNLSGROUPBYDATEFLAG AS JRNLS_GROUP_BY_DATE_FLAG,
    LEDGERLASTUPDATEDATE AS LAST_UPDATE_DT,
    LEDGERLASTUPDATELOGIN AS LAST_UPDATE_LOGIN,
    LEDGERLASTUPDATEDBY AS LAST_UPDATED_BY,
    LEDGERLATESTENCUMBRANCEYEAR AS LATEST_ENCUMBRANCE_YEAR,
    LEDGERLATESTOPENEDPERIODNAME AS LATEST_OPENED_PERIOD_NAME,
    LEDGERLEDGERCATEGORYCODE AS LEDGER_CATEGORY_CODE,
    LEDGERCURRENCYCODE AS LEDGER_CURR_CODE,
    LEDGERLEDGERID AS LEDGER_ID,
    LEDGERNAME AS LEDGER_NAME,
    LEDGERPERIODENDRATETYPE AS LEDGER_PERIOD_END_RATE_TYPE,
    LEDGEROBJECTTYPECODE AS LEDGER_TYPE,
    LEDGERMINIMUMTHRESHOLDAMOUNT AS MINIMUM_THRESHOLD_AMOUNT,
    LEDGERNETCLOSINGBALFLAG AS NET_CLOSING_BAL_FLAG,
    LEDGERNETINCOMECODECOMBINATIONID AS NET_INCOME_CODE_COMBINATION_ID,
    LEDGEROBJECTVERSIONNUMBER AS OBJECT_VERSION_NUMBER,
    LEDGERPERIODAVERAGERATETYPE AS PERIOD_AVERAGE_RATE_TYPE,
    LEDGERPERIODSETNAME AS PERIOD_SET_NAME,
    LEDGERPOPUPSTATACCOUNTFLAG AS POPUPSTAT_ACCOUNT_FLAG,
    LEDGERPRIORPRDNOTIFICATIONFLAG AS PRIOR_PRD_NOTIFICATION_FLAG,
    LEDGERREQUIREBUDGETJOURNALSFLAG AS REQUIRE_BUDGET_JOURNALS_FLAG,
    LEDGERRESENCUMBCODECOMBINATIONID AS RESENCUMB_CODE_COMBINATION_ID,
    LEDGERRETEARNCODECOMBINATIONID AS RETEARN_CODE_COMBINATION_ID,
    LEDGERREVALFROMPRILGRCURR AS REVAL_FROM_PRILGR_CURR,
    LEDGERROUNDINGCODECOMBINATIONID AS ROUNDING_CODE_COMBINATION_ID,
    LEDGERSEQUENCINGMODECODE AS SEQUENCING_MODE_CODE,
    LEDGERSHORTNAME AS SHORT_NAME,
    LEDGERSLAACCOUNTINGMETHODCODE AS SLA_ACCOUNTING_METHOD_CODE,
    LEDGERSLAACCOUNTINGMETHODTYPE AS SLA_ACCOUNTING_METHOD_TYPE,
    LEDGERSLABALBYLEDGERCURRFLAG AS SLA_BAL_BY_LEDGER_CURR_FLAG,
    LEDGERSLADESCRIPTIONLANGUAGE AS SLA_DESCRIPTION_LANGUAGE,
    LEDGERSLAENTEREDCURBALSUSCCID AS SLA_ENTERED_CUR_BALSUSCC_ID,
    LEDGERSLALEDGERCASHBASISFLAG AS SLA_LEDGER_CASH_BASIS_FLAG,
    LEDGERSLALEDGERCURBALSUSCCID AS SLA_LEDGER_CURBALSUS_CC_ID,
    LEDGERSLASEQUENCINGFLAG AS SLA_SEQUENCING_FLAG,
    LEDGERSUSPENSEALLOWEDFLAG AS SUSPENSE_ALLOWED_FLAG,
    LEDGERTHRESHOLDAMOUNT AS THRESHOLD_AMOUNT,
    LEDGERTRACKROUNDINGIMBALANCEFLAG AS TRACK_ROUNDING_IMBALANCE_FLAG,
    LEDGERTRANSACTIONCALENDARID AS TRANSACTION_CALENDAR_ID,
    LEDGERTRANSLATEEODFLAG AS TRANSLATE_EOD_FLAG,
    LEDGERTRANSLATEQATDFLAG AS TRANSLATE_QATD_FLAG,
    LEDGERTRANSLATEYATDFLAG AS TRANSLATE_YATD_FLAG,
    LEDGERVALIDATEJOURNALREFDATE AS VALIDATE_JOURNAL_REF_DATE FROM raw.fscmtopmodelam_finextractam_glbiccextractam_ledgerextractpvo
),
budgetbalanceextract AS (
    SELECT BUDGETNAME AS BUDGET_NAME,
    GLBUDBALCREATEDBY AS CREATED_BY,
    GLBUDBALCREATIONDATE AS CREATION_DT,
    CURRENCYCODE AS CURRENCY_CODE,
    CURRENCYTYPE AS CURRENCY_TYPE,
    GLBUDBALLASTUPDATEDATE AS LAST_UPDATE_DT,
    GLBUDBALLASTUPDATELOGIN AS LAST_UPDATE_LOGIN,
    GLBUDBALLASTUPDATEDBY AS LAST_UPDATED_BY,
    LEDGERID AS LEDGER_ID,
    GLBUDBALOBJECTVERSIONNUMBER AS OBJ_VERSION_NO,
    PERIODNAME AS PERIOD_NAME,
    GLBUDBALPERIODNETCR AS PERIOD_NET_CR,
    GLBUDBALPERIODNETDR AS PERIOD_NET_DR,
    GLBUDBALSEGMENT1 AS SEGMENT1,
    GLBUDBALSEGMENT10 AS SEGMENT10,
    GLBUDBALSEGMENT11 AS SEGMENT11,
    GLBUDBALSEGMENT12 AS SEGMENT12,
    GLBUDBALSEGMENT13 AS SEGMENT13,
    GLBUDBALSEGMENT14 AS SEGMENT14,
    GLBUDBALSEGMENT15 AS SEGMENT15,
    GLBUDBALSEGMENT16 AS SEGMENT16,
    GLBUDBALSEGMENT17 AS SEGMENT17,
    GLBUDBALSEGMENT18 AS SEGMENT18,
    GLBUDBALSEGMENT19 AS SEGMENT19,
    GLBUDBALSEGMENT2 AS SEGMENT2,
    GLBUDBALSEGMENT20 AS SEGMENT20,
    GLBUDBALSEGMENT21 AS SEGMENT21,
    GLBUDBALSEGMENT22 AS SEGMENT22,
    GLBUDBALSEGMENT23 AS SEGMENT23,
    GLBUDBALSEGMENT24 AS SEGMENT24,
    GLBUDBALSEGMENT25 AS SEGMENT25,
    GLBUDBALSEGMENT26 AS SEGMENT26,
    GLBUDBALSEGMENT27 AS SEGMENT27,
    GLBUDBALSEGMENT28 AS SEGMENT28,
    GLBUDBALSEGMENT29 AS SEGMENT29,
    GLBUDBALSEGMENT3 AS SEGMENT3,
    GLBUDBALSEGMENT30 AS SEGMENT30,
    GLBUDBALSEGMENT4 AS SEGMENT4,
    GLBUDBALSEGMENT5 AS SEGMENT5,
    GLBUDBALSEGMENT6 AS SEGMENT6,
    GLBUDBALSEGMENT7 AS SEGMENT7,
    GLBUDBALSEGMENT8 AS SEGMENT8,
    GLBUDBALSEGMENT9 AS SEGMENT9 FROM raw.fscmtopmodelam_finextractam_glbiccextractam_budgetbalanceextractpvo
),
codecombinationextract AS (
    SELECT CODECOMBINATIONACCOUNTTYPE AS ACCOUNT_TYPE,
    CODECOMBINATIONCHARTOFACCOUNTSID AS COA_ID,
    CODECOMBINATIONCREATEDBY AS CREATED_BY,
    CODECOMBINATIONCREATIONDATE AS CREATION_DT,
    CODECOMBINATIONENABLEDFLAG AS ENABLED_FLAG,
    CODECOMBINATIONENDDATEACTIVE AS END_DT_ACTIVE,
    CODECOMBINATIONCODECOMBINATIONID AS INTEGRATION_ID,
    CODECOMBINATIONLASTUPDATEDATE AS LAST_UPDATE_DT,
    CODECOMBINATIONLASTUPDATEDBY AS LAST_UPDATED_BY,
    CODECOMBINATIONSEGMENT1 AS SEGMENT1,
    CODECOMBINATIONSEGMENT2 AS SEGMENT2,
    CODECOMBINATIONSEGMENT3 AS SEGMENT3,
    CODECOMBINATIONSEGMENT4 AS SEGMENT4,
    CODECOMBINATIONSEGMENT5 AS SEGMENT5,
    CODECOMBINATIONSEGMENT6 AS SEGMENT6,
    CODECOMBINATIONSEGMENT7 AS SEGMENT7,
    CODECOMBINATIONSEGMENT8 AS SEGMENT8,
    CODECOMBINATIONSEGMENT9 AS SEGMENT9,
    CODECOMBINATIONSTARTDATEACTIVE AS START_DT_ACTIVE,
    CODECOMBINATIONSUMMARYFLAG AS SUMMARY_FLAG FROM raw.fscmtopmodelam_finextractam_glbiccextractam_codecombinationextractpvo
),
codecombination AS (
    SELECT CODECOMBINATIONACCOUNTTYPE AS ACCOUNT_TYPE,
    CODECOMBINATIONCHARTOFACCOUNTSID AS COA_ID,
    CODECOMBINATIONCREATEDBY AS CREATED_BY,
    CODECOMBINATIONCREATIONDATE AS CREATION_DT,
    CODECOMBINATIONENABLEDFLAG AS ENABLED_FLAG,
    CODECOMBINATIONENDDATEACTIVE AS END_DT_ACTIVE,
    CODECOMBINATIONCODECOMBINATIONID AS INTEGRATION_ID,
    CODECOMBINATIONLASTUPDATEDATE AS LAST_UPDATE_DT,
    CODECOMBINATIONLASTUPDATEDBY AS LAST_UPDATED_BY,
    CODECOMBINATIONSEGMENT1 AS SEGMENT1,
    CODECOMBINATIONSEGMENT2 AS SEGMENT2,
    CODECOMBINATIONSEGMENT3 AS SEGMENT3,
    CODECOMBINATIONSEGMENT4 AS SEGMENT4,
    CODECOMBINATIONSEGMENT5 AS SEGMENT5,
    CODECOMBINATIONSEGMENT6 AS SEGMENT6,
    CODECOMBINATIONSEGMENT7 AS SEGMENT7,
    CODECOMBINATIONSEGMENT8 AS SEGMENT8,
    CODECOMBINATIONSEGMENT9 AS SEGMENT9,
    CODECOMBINATIONSTARTDATEACTIVE AS START_DT_ACTIVE,
    CODECOMBINATIONSUMMARYFLAG AS SUMMARY_FLAG FROM raw.fscmtopmodelam_finextractam_glbiccextractam_codecombinationextractpvo
),
CT_GL_BUDGETS_F_STG AS (
  SELECT 
    CAST(Bal.LEDGER_ID AS STRING) AS LEDGER_ID, 
    CAST(Bal.GLCC_ID AS STRING) AS GLCC_ID, 
    CAST(CONCAT(Period.PERIOD_NAME, '~', Period.PERIOD_SET_NAME) AS STRING) AS PERIOD_ID, 
    CAST(( IFNULL(
        CAST( CAST( 
        CONCAT(
        CAST(EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', Period.PERIOD_DT_ID)) AS STRING), 
                LPAD(CAST(EXTRACT(MONTH FROM SAFE.PARSE_DATE('%Y-%m-%d', Period.PERIOD_DT_ID)) AS STRING), 2, '0'), 
                LPAD(CAST(EXTRACT(DAY FROM SAFE.PARSE_DATE('%Y-%m-%d', Period.PERIOD_DT_ID)) AS STRING), 2, '0')
        )  
        AS INT64)      
        AS FLOAT64)    
        ,0)       
        AS STRING))
     AS BUDGET_MONTH_ID, 
    Period.PERIOD_NAME AS BUDGET_MONTH, 
    IFNULL(A.BUDGET_NAME_1, 'Budget') AS BUDGET_NAME, 
    Ledger.LEDGER_NAME AS LEDGER_NAME, 
    Bal.CURRENCY_CODE AS CURRENCY_CODE, 
    CC.SEGMENT1 AS SEGMENT1, 
    CC.SEGMENT2 AS SEGMENT2, 
    CC.SEGMENT3 AS SEGMENT3, 
    CC.SEGMENT4 AS SEGMENT4, 
    CC.SEGMENT5 AS SEGMENT5, 
    CC.SEGMENT6 AS SEGMENT6, 
    CC.SEGMENT7 AS SEGMENT7, 
    CC.SEGMENT8 AS SEGMENT8, 
    SUM(IFNULL(CAST(Bal.PERIOD_NET_CR AS FLOAT64), 0)) AS PERIOD_NET_DR, 
    SUM(IFNULL(CAST(Bal.PERIOD_NET_DR AS FLOAT64), 0)) AS PERIOD_NET_CR, 
    SUM(IFNULL(CAST(Bal.PERIOD_NET_CR AS FLOAT64), 0) - IFNULL(CAST(Bal.PERIOD_NET_DR AS FLOAT64), 0)) AS AMOUNT, 
    MAX(IFNULL(A.CREATION_DT_1, Bal.LAST_UPDATE_DT)) AS CREATION_DT, 
    MAX(IFNULL(A.LAST_UPDATE_DT_1, Bal.LAST_UPDATE_DT)) AS LAST_UPDATE_DT, 
    MAX(IFNULL(A.CREATED_BY_1, Bal.LAST_UPDATED_BY)) AS CREATED_BY, 
    MAX(IFNULL(A.LAST_UPDATED_BY_1, Bal.LAST_UPDATED_BY)) AS LAST_UPDATED_BY, 
    IFNULL(NULL, 'Standard') AS TYPE, 
    IFNULL(NULL, '') AS SCENARIO, 
    CAST(A.OBJ_VERSION_NO_1 AS STRING) AS VERSION, 
    Ledger.LEDGER_CURR_CODE AS CURRENCY, 
    IFNULL(NULL, '') AS MGMT_REPORTING_LINE, 
    IFNULL(NULL, '') AS VEW, 
    IFNULL(NULL, '') AS DATA_LOAD_CUBE_NAME, 
    CONCAT(Bal.LEDGER_ID, '~', Bal.GLCC_ID, '~', Period.PERIOD_NAME, '~', Bal.CURRENCY_CODE) AS INTEGRATION_ID, 
    1000 AS DATASOURCE_NUM_ID 
  FROM 
    balanceextract AS Bal 
    INNER JOIN fiscalperiodextract AS Period ON 1 = 1
    INNER JOIN ledgerextractpvo AS Ledger ON 1 = 1 
    INNER JOIN codecombinationextract AS CC ON Bal.PERIOD_NAME = Period.PERIOD_NAME 
    AND Bal.LEDGER_ID = Ledger.LEDGER_ID 
    AND Bal.GLCC_ID = CC.INTEGRATION_ID 
    AND Ledger.PERIOD_SET_NAME = Period.PERIOD_SET_NAME 
    AND Bal.ACTUAL_FLAG IN ('A', 'B') 
    LEFT OUTER JOIN (
      SELECT 
        CCC.INTEGRATION_ID AS INTEGRATION_ID, 
        CCC.SEGMENT1 AS SEGMENT1, 
        CCC.SEGMENT2 AS SEGMENT2, 
        CCC.SEGMENT3 AS SEGMENT3, 
        CCC.SEGMENT4 AS SEGMENT4, 
        CCC.SEGMENT5 AS SEGMENT5,
        Budject.BUDGET_NAME AS BUDGET_NAME, 
        Budject.CURRENCY_CODE AS CURRENCY_CODE, 
        Budject.CURRENCY_TYPE AS CURRENCY_TYPE, 
        Budject.CREATED_BY AS CREATED_BY, 
        Budject.CREATION_DT AS CREATION_DT, 
        Budject.LAST_UPDATE_DT AS LAST_UPDATE_DT, 
        Budject.LAST_UPDATE_LOGIN AS LAST_UPDATE_LOGIN, 
        Budject.LAST_UPDATED_BY AS LAST_UPDATED_BY, 
        Budject.OBJ_VERSION_NO AS OBJ_VERSION_NO, 
        Budject.PERIOD_NET_CR AS PERIOD_NET_CR, 
        Budject.PERIOD_NET_DR AS PERIOD_NET_DR, 
        Budject.SEGMENT1 AS budject_SEGMENT1, 
        Budject.SEGMENT10 AS SEGMENT10, 
        Budject.SEGMENT11 AS SEGMENT11, 
        Budject.SEGMENT12 AS SEGMENT12, 
        Budject.SEGMENT13 AS SEGMENT13, 
        Budject.SEGMENT14 AS SEGMENT14, 
        Budject.SEGMENT15 AS SEGMENT15, 
        Budject.SEGMENT16 AS SEGMENT16, 
        Budject.SEGMENT17 AS SEGMENT17, 
        Budject.SEGMENT18 AS SEGMENT18, 
        Budject.SEGMENT19 AS SEGMENT19, 
        Budject.SEGMENT2 AS budject_SEGMENT2, 
        Budject.SEGMENT20 AS SEGMENT20, 
        Budject.SEGMENT21 AS SEGMENT21, 
        Budject.SEGMENT22 AS SEGMENT22, 
        Budject.SEGMENT23 AS SEGMENT23, 
        Budject.SEGMENT24 AS SEGMENT24, 
        Budject.SEGMENT25 AS SEGMENT25, 
        Budject.SEGMENT26 AS SEGMENT26, 
        Budject.SEGMENT27 AS SEGMENT27, 
        Budject.SEGMENT28 AS SEGMENT28, 
        Budject.SEGMENT29 AS SEGMENT29, 
        Budject.SEGMENT3 AS budject_SEGMENT3, 
        Budject.SEGMENT30 AS SEGMENT30, 
        Budject.SEGMENT4 AS budject_SEGMENT4, 
        Budject.SEGMENT5 AS budject_SEGMENT5, 
        Budject.SEGMENT6 AS SEGMENT6, 
        Budject.SEGMENT7 AS SEGMENT7, 
        Budject.SEGMENT8 AS SEGMENT8, 
        Budject.SEGMENT9 AS SEGMENT9, 
        Budject.LEDGER_ID AS LEDGER_ID, 
        Budject.PERIOD_NAME AS PERIOD_NAME, 
        Budject.BUDGET_NAME AS BUDGET_NAME_1, 
        Budject.CREATED_BY AS CREATED_BY_1, 
        Budject.CREATION_DT AS CREATION_DT_1, 
        Budject.LAST_UPDATE_DT AS LAST_UPDATE_DT_1, 
        Budject.LAST_UPDATED_BY AS LAST_UPDATED_BY_1, 
        Budject.OBJ_VERSION_NO AS OBJ_VERSION_NO_1, 
        CCC.INTEGRATION_ID AS INTEGRATION_ID_1 
      FROM 
        codecombination AS CCC 
        JOIN budgetbalanceextract AS Budject 
        ON Budject.SEGMENT1 = CCC.SEGMENT1 
        AND Budject.SEGMENT2 = CCC.SEGMENT2 
        AND Budject.SEGMENT3 = CCC.SEGMENT3 
        AND Budject.SEGMENT4 = CCC.SEGMENT4 
        AND Budject.SEGMENT5 = CCC.SEGMENT5
    ) A ON Bal.GLCC_ID = A.INTEGRATION_ID_1 
  GROUP BY 
    Bal.LEDGER_ID, 
    Bal.GLCC_ID, 
    CONCAT(Period.PERIOD_NAME, '~', Period.PERIOD_SET_NAME), 
    CAST(
      IFNULL(
        CAST(
          CONCAT(
            CAST(EXTRACT(YEAR FROM SAFE.PARSE_DATE('%Y-%m-%d', Period.PERIOD_DT_ID)) AS STRING), 
            LPAD(CAST(EXTRACT(MONTH FROM SAFE.PARSE_DATE('%Y-%m-%d', Period.PERIOD_DT_ID)) AS STRING), 2, '0'), 
            LPAD(CAST(EXTRACT(DAY FROM SAFE.PARSE_DATE('%Y-%m-%d', Period.PERIOD_DT_ID)) AS STRING), 2, '0')
          ) AS INT64
        , '0') AS STRING
      , '0') AS STRING
    ), 
    Period.PERIOD_NAME, 
    IFNULL(A.BUDGET_NAME_1, 'Budget'), 
    Ledger.LEDGER_NAME, 
    Bal.CURRENCY_CODE, 
    CC.SEGMENT1, 
    CC.SEGMENT2, 
    CC.SEGMENT3, 
    CC.SEGMENT4, 
    CC.SEGMENT5, 
    CC.SEGMENT6, 
    CC.SEGMENT7, 
    CC.SEGMENT8, 
    A.OBJ_VERSION_NO_1, 
    Ledger.LEDGER_CURR_CODE, 
    CONCAT(Bal.LEDGER_ID, '~', Bal.GLCC_ID, '~', Period.PERIOD_NAME, '~', Bal.CURRENCY_CODE)
)

SELECT 
  * 
FROM 
  CT_GL_BUDGETS_F_STG
