with balanceextract AS ( SELECT * FROM  raw.fscmtopmodelam_finextractam_glbiccextractam_balanceextractpvo ),
fiscalperiodextract AS ( SELECT * FROM  raw.fscmtopmodelam_finextractam_glbiccextractam_fiscalperiodextractpvo ),
ledgerextractpvo AS ( SELECT * FROM  raw.fscmtopmodelam_finextractam_glbiccextractam_ledgerextractpvo),
budgetbalanceextract AS ( SELECT * FROM  raw.fscmtopmodelam_finextractam_glbiccextractam_budgetbalanceextractpvo),
codecombinationextract AS ( SELECT * FROM  raw.fscmtopmodelam_finextractam_glbiccextractam_codecombinationextractpvo),
codecombination AS ( SELECT * FROM  raw.fscmtopmodelam_finextractam_glbiccextractam_codecombinationextractpvo),
CT_GL_BUDGETS_F_STG AS (
select 
  cast(Bal.LEDGER_ID as varchar) AS LEDGER_ID, 
  cast(Bal.GLCC_ID as varchar) AS GLCC_ID, 
  cast(
    Period.PERIOD_NAME + '~' + Period.PERIOD_SET_NAME as varchar
  ) AS PERIOD_ID, 
  CAST(
    ISNULL(
      CAST(
        CAST(
          CONCAT(
            YEAR(Period.PERIOD_DT_ID), 
            RIGHT(
              '00' + CAST(
                MONTH(Period.PERIOD_DT_ID) AS VARCHAR
              ), 
              2
            ), 
            RIGHT(
              '00' + CAST(
                DAY(Period.PERIOD_DT_ID) AS VARCHAR
              ), 
              2
            )
          ) AS INT
        ) AS DECIMAL
      ), 
      0
    ) AS VARCHAR
  ) AS BUDGET_MONTH_ID, 
  Period.PERIOD_NAME AS BUDGET_MONTH, 
  (
    isnull(A.BUDGET_NAME_1, 'Budget')
  ) AS BUDGET_NAME, 
  Ledger.LEDGER_NAME AS LEDGER_NAME, 
  Bal.CURRENCY_CODE AS CURRENCY_CODE, 
  CAST(CC.SEGMENT1 as varchar) AS SEGMENT1, 
  CAST(CC.SEGMENT2 as varchar) AS SEGMENT2, 
  CAST(CC.SEGMENT3 as varchar) AS SEGMENT3, 
  CC.SEGMENT4 AS SEGMENT4, 
  CAST(CC.SEGMENT5 as varchar) AS SEGMENT5, 
  CC.SEGMENT6 AS SEGMENT6, 
  CC.SEGMENT7 AS SEGMENT7, 
  CC.SEGMENT8 AS SEGMENT8, 
  (
    sum(
      isnull(
        cast(Bal.PERIOD_NET_CR as float), 
        0
      )
    )
  ) AS PERIOD_NET_DR, 
  (
    SUM(
      isnull(
        cast(Bal.PERIOD_NET_DR as float), 
        0
      )
    )
  ) AS PERIOD_NET_CR, 
  (
    SUM(
      isnull(
        cast(Bal.PERIOD_NET_CR as float), 
        0
      ) - isnull(
        cast(Bal.PERIOD_NET_DR as float), 
        0
      )
    )
  ) AS AMOUNT, 
  (
    MAX(
      isnull(
        A.CREATION_DT_1, Bal.LAST_UPDATE_DT
      )
    )
  ) AS CREATION_DT, 
  (
    MAX(
      isnull(
        A.LAST_UPDATE_DT_1, Bal.LAST_UPDATE_DT
      )
    )
  ) AS LAST_UPDATE_DT, 
  (
    MAX(
      isnull(
        A.CREATED_BY_1, Bal.LAST_UPDATED_BY
      )
    )
  ) AS CREATED_BY, 
  (
    MAX(
      isnull(
        A.LAST_UPDATED_BY_1, Bal.LAST_UPDATED_BY
      )
    )
  ) AS LAST_UPDATED_BY, 
  isnull(NULL, 'Standard') AS TYPE, 
  isnull(NULL, '') AS SCENARIO, 
  CAST(A.OBJ_VERSION_NO_1 as varchar) AS VERSION, 
  Ledger.LEDGER_CURR_CODE AS CURRENCY, 
  isnull(NULL, '') AS MGMT_REPORTING_LINE, 
  isnull(NULL, '') AS VEW, 
  isnull(NULL, '') AS DATA_LOAD_CUBE_NAME, 
  (
    Bal.LEDGER_ID + '~' + Bal.GLCC_ID + '~' + Period.PERIOD_NAME + '~' + Bal.CURRENCY_CODE
  ) AS INTEGRATION_ID, 
  1000 AS DATASOURCE_NUM_ID 
from 
  balanceextract 
     as Bal 
  INNER JOIN fiscalperiodextract, 
    as Period ON 1 = 1 
	
  INNER JOIN ledgerextractpvo, 
     as Ledger ON 1 = 1 
  INNER JOIN codecombinationextract, 
   as CC ON Bal.PERIOD_NAME = Period.PERIOD_NAME 
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
     codecombination, 
         as CCC, 
      budgetbalanceextract, 
         as Budject 
    WHERE 
      (
        Budject.SEGMENT1 = CCC.SEGMENT1 
        AND Budject.SEGMENT2 = CCC.SEGMENT2 
        AND Budject.SEGMENT3 = CCC.SEGMENT3 
        AND Budject.SEGMENT4 = CCC.SEGMENT4 
        AND Budject.SEGMENT5 = CCC.SEGMENT5
      )
  ) A ON Bal.GLCC_ID = A.INTEGRATION_ID_1 
where 
  (1 = 1) 
GROUP BY 
  Bal.LEDGER_ID, 
  Bal.GLCC_ID, 
  (
    Period.PERIOD_NAME + '~' + Period.PERIOD_SET_NAME
  ), 
  CAST(
    ISNULL(
      CAST(
        CAST(
          CONCAT(
            YEAR(Period.PERIOD_DT_ID), 
            RIGHT(
              '00' + CAST(
                MONTH(Period.PERIOD_DT_ID) AS VARCHAR
              ), 
              2
            ), 
            RIGHT(
              '00' + CAST(
                DAY(Period.PERIOD_DT_ID) AS VARCHAR
              ), 
              2
            )
          ) AS INT
        ) AS DECIMAL
      ), 
      0
    ) AS VARCHAR
  ), 
  Period.PERIOD_NAME, 
  isnull(A.BUDGET_NAME_1, 'Budget'), 
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
  (
    Bal.LEDGER_ID + '~' + Bal.GLCC_ID + '~' + Period.PERIOD_NAME + '~' + Bal.CURRENCY_CODE)
	
	 )
  SELECT * FROM CT_GL_BUDGETS_F_STG
