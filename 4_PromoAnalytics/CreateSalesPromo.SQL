-- This script joins Sales_v2 table with Promotions table
-- One consideration is that a SKU for a STR is forced to have just one promotion for a week
-- If there are more than one promotion for a week at a SKU-STR level, then the promotion that has
-- higher MKUP/MKDN is considered

SET
  @@dataset_project_id = 'analytics-askuity-thd';
SET
  @@dataset_id = 'sid_workspace';
  -- Very useful link for multi statement guide
  -- https://cloud.google.com/bigquery/docs/multi-statement-queries
BEGIN
CREATE OR REPLACE VIEW
  SALES_PROMO_2022 AS (
  WITH
    PROMO AS (
    SELECT
      SKU_NBR,
      STR_NBR,
      PROMO_ID,
      PROMO_RTL_AMT,
      PROMO_NM,
      PROMO_TYP,
      PROMO_S_DESC,
      PROMO_DESC,
      SRC_PROMO_BGN_TS,
      SRC_PROMO_END_TS,
      MKUP_MKDN_AMT,
      MKUP_MKDN_PCT,
      EFF_RTL_AMT,
      SRC_EFF_BGN_TS,
      SRC_EFF_END_TS,
      SRC_LAST_UPD_TS,
      CLR_FLG,
      SRC_DEL_FLG,
      SRC_DEL_DT -- What does del at source mean?
    FROM
      pr-edw-views-thd.SHARED_COMMONS.SKU_STR_RETL_PROMO
    WHERE
      DATE(SRC_PROMO_BGN_TS)>'2022-01-01' -- Looking at promotions this year
      AND PROMO_ID IS NOT NULL ),
    SALES AS(
    SELECT
      SLS_DT,
      STR_NBR,
      MVNDR_NBR,
      SKU_NBR,
      TY_SLS,
      TY_SLS_UNITS,
      TY_RETURNS,
      TY_RETURN_UNITS
    FROM
      pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SALES_v2
    WHERE
      DATE(SLS_DT)>'2022-01-01'
      AND ((TY_SLS IS NOT NULL)-- conditions to remove rows where both TY_SLS AND RETURNS ARE 0 or NULL
        OR (TY_RETURNS IS NOT NULL))
      AND ((TY_RETURNS != 0)
        OR (TY_SLS IS NOT NULL))),
    SKU_DESC AS (
    SELECT
      SKU_NBR,
      SKU_DESC
    FROM (
      SELECT
        SKU_NBR,
        RANK() OVER (PARTITION BY SKU_NBR ORDER BY SKU_CRT_DT DESC) AS rank_to_latest,
        SKU_DESC
      FROM
        `pr-edw-views-thd.SHARED_COMMONS.SKU` QUALIFY rank_to_latest=1))
  SELECT
    SALES.SKU_NBR,
    SKU_DESC.SKU_DESC,
    SALES.STR_NBR,
    SALES.SLS_DT,
    CASE
      WHEN EXTRACT(DAYOFWEEK FROM SALES.SLS_DT) = 1 THEN SALES.SLS_DT
      ELSE
        DATE_ADD(SALES.SLS_DT, INTERVAL (8 - EXTRACT(DAYOFWEEK FROM SALES.SLS_DT))DAY)
    END AS SLS_WEEK,
    SALES.MVNDR_NBR,
    SALES.TY_SLS,
    SALES.TY_SLS_UNITS,
    SALES.TY_RETURNS,
    SALES.TY_RETURN_UNITS,
    PROMO_ID,
    PROMO_RTL_AMT,
    PROMO_NM,
    PROMO_TYP,
    PROMO_S_DESC,
    PROMO_DESC,
    SRC_PROMO_BGN_TS,
    SRC_PROMO_END_TS,
    MKUP_MKDN_AMT,
    MKUP_MKDN_PCT,
    EFF_RTL_AMT,
    SRC_EFF_BGN_TS,
    SRC_EFF_END_TS,
    SRC_LAST_UPD_TS,
    CLR_FLG,
    SRC_DEL_FLG,
    SRC_DEL_DT, -- What does del at source mean?
    CASE
      WHEN PROMO_ID IS NULL THEN 0
    ELSE
    1
  END
    AS PROMO_FLG,
    ROW_NUMBER() OVER (PARTITION BY SALES.SLS_DT, SALES.SKU_NBR, SALES.STR_NBR ORDER BY MKUP_MKDN_PCT DESC) AS RNK_MOST_DISC
  FROM
    SALES
  LEFT JOIN
    PROMO
  ON
    (SALES.SKU_NBR = PROMO.SKU_NBR
      AND SALES.STR_NBR =  PROMO.STR_NBR
      AND SALES.SLS_DT >= PROMO.SRC_PROMO_BGN_TS
      AND SALES.SLS_DT <= PROMO.SRC_PROMO_END_TS
      AND SALES.SLS_DT <= PROMO.SRC_EFF_END_TS)
  LEFT JOIN
    SKU_DESC
  ON
    (PROMO.SKU_NBR = SKU_DESC.SKU_NBR)
  WHERE
    SRC_DEL_FLG IS NOT TRUE
    QUALIFY RNK_MOST_DISC = 1) ;
END
  ;

BEGIN
CREATE OR REPLACE TABLE
  SALES_PROMO_2022 AS (
  WITH
    PROMO AS (
    SELECT
      SKU_NBR,
      STR_NBR,
      PROMO_ID,
      PROMO_RTL_AMT,
      PROMO_NM,
      PROMO_TYP,
      PROMO_S_DESC,
      PROMO_DESC,
      SRC_PROMO_BGN_TS,
      SRC_PROMO_END_TS,
      MKUP_MKDN_AMT,
      MKUP_MKDN_PCT,
      EFF_RTL_AMT,
      SRC_EFF_BGN_TS,
      SRC_EFF_END_TS,
      SRC_LAST_UPD_TS,
      CLR_FLG,
      SRC_DEL_FLG,
      SRC_DEL_DT -- What does del at source mean?
    FROM
      pr-edw-views-thd.SHARED_COMMONS.SKU_STR_RETL_PROMO
    WHERE
      DATE(SRC_PROMO_BGN_TS)>'2022-01-01' -- Looking at promotions this year
      AND PROMO_ID IS NOT NULL ),
    SALES AS(
    SELECT
      SLS_DT,
      STR_NBR,
      MVNDR_NBR,
      SKU_NBR,
      TY_SLS,
      TY_SLS_UNITS,
      TY_RETURNS,
      TY_RETURN_UNITS
    FROM
      pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SALES_v2
    WHERE
      DATE(SLS_DT)>'2022-01-01'
      AND ((TY_SLS IS NOT NULL)-- conditions to remove rows where both TY_SLS AND RETURNS ARE 0 or NULL
        OR (TY_RETURNS IS NOT NULL))
      AND ((TY_RETURNS != 0)
        OR (TY_SLS IS NOT NULL))),
    SKU_DESC AS (
    SELECT
      SKU_NBR,
      SKU_DESC
    FROM (
      SELECT
        SKU_NBR,
        RANK() OVER (PARTITION BY SKU_NBR ORDER BY SKU_CRT_DT DESC) AS rank_to_latest,
        SKU_DESC
      FROM
        `pr-edw-views-thd.SHARED_COMMONS.SKU` QUALIFY rank_to_latest=1))
  SELECT
    SALES.SKU_NBR,
    SKU_DESC.SKU_DESC,
    SALES.STR_NBR,
    SALES.SLS_DT,
    CASE
      WHEN EXTRACT(DAYOFWEEK FROM SALES.SLS_DT) = 1 THEN SALES.SLS_DT
      ELSE
        DATE_ADD(SALES.SLS_DT, INTERVAL (8 - EXTRACT(DAYOFWEEK FROM SALES.SLS_DT))DAY)
    END AS SLS_WEEK,
    SALES.MVNDR_NBR,
    SALES.TY_SLS,
    SALES.TY_SLS_UNITS,
    SALES.TY_RETURNS,
    SALES.TY_RETURN_UNITS,
    PROMO_ID,
    PROMO_RTL_AMT,
    PROMO_NM,
    PROMO_TYP,
    PROMO_S_DESC,
    PROMO_DESC,
    SRC_PROMO_BGN_TS,
    SRC_PROMO_END_TS,
    MKUP_MKDN_AMT,
    MKUP_MKDN_PCT,
    EFF_RTL_AMT,
    SRC_EFF_BGN_TS,
    SRC_EFF_END_TS,
    SRC_LAST_UPD_TS,
    CLR_FLG,
    SRC_DEL_FLG,
    SRC_DEL_DT, -- What does del at source mean?
    CASE
      WHEN PROMO_ID IS NULL THEN 0
    ELSE
    1
  END
    AS PROMO_FLG,
    ROW_NUMBER() OVER (PARTITION BY SALES.SLS_DT, SALES.SKU_NBR, SALES.STR_NBR ORDER BY MKUP_MKDN_PCT DESC) AS RNK_MOST_DISC
  FROM
    SALES
  LEFT JOIN
    PROMO
  ON
    (SALES.SKU_NBR = PROMO.SKU_NBR
      AND SALES.STR_NBR =  PROMO.STR_NBR
      AND SALES.SLS_DT >= PROMO.SRC_PROMO_BGN_TS
      AND SALES.SLS_DT <= PROMO.SRC_PROMO_END_TS
      AND SALES.SLS_DT <= PROMO.SRC_EFF_END_TS)
  LEFT JOIN
    SKU_DESC
  ON
    (PROMO.SKU_NBR = SKU_DESC.SKU_NBR)
  WHERE
    SRC_DEL_FLG IS NOT TRUE
    QUALIFY RNK_MOST_DISC = 1) ;
END
  ;