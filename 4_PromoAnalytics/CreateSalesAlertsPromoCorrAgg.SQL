  -- Script to combine sales promo data with alerts and correlation table
  -- In addition, a couple of metrics were also built and finally a visualisation table for looker studio was built
SET
  @@dataset_project_id = 'analytics-askuity-thd';
SET
  @@dataset_id = 'sid_workspace'; 

-- Aggregating Sales Promotion data to generate metrics like
-- Number of stores that had promotion going on at a SKU-WEEK level
CREATE OR REPLACE VIEW analytics-askuity-thd.sid_workspace.SALES_PROMO_AGG AS
SELECT 
  SALES_PROMO.*,
  CNT_PROMO AS DISTINCT_PROMOS,
  TOTAL_STORES,
  TOTAL_STORES_W_PROMO,
  TOTAL_STORES_W_PROMO/TOTAL_STORES AS RATIO_STR_PROMO -- Creating metric ratio of stores having promotions
FROM
  analytics-askuity-thd.sid_workspace.SALES_PROMO_2022 AS SALES_PROMO
LEFT JOIN
(SELECT 
  SLS_WEEK,
  SKU_NBR,
  COUNT(DISTINCT PROMO_ID) CNT_PROMO,
  COUNT(DISTINCT STR_NBR) AS TOTAL_STORES
FROM
  analytics-askuity-thd.sid_workspace.SALES_PROMO_2022
GROUP BY
  SLS_WEEK,
  SKU_NBR) AS TOTAL_STORES_TBL
ON (SALES_PROMO.SLS_WEEK = TOTAL_STORES_TBL.SLS_WEEK
    AND SALES_PROMO.SKU_NBR = TOTAL_STORES_TBL.SKU_NBR)
LEFT JOIN
(SELECT 
  SLS_WEEK,
  SKU_NBR,
  COUNT(DISTINCT STR_NBR) AS TOTAL_STORES_W_PROMO
FROM
  analytics-askuity-thd.sid_workspace.SALES_PROMO_2022
WHERE
  PROMO_FLG = 1 
GROUP BY
  SLS_WEEK,
  SKU_NBR) AS PROMO_STORES
ON (SALES_PROMO.SLS_WEEK=PROMO_STORES.SLS_WEEK
    AND SALES_PROMO.SKU_NBR=PROMO_STORES.SKU_NBR) ;

-- Certain SKUs have multiple alerts for the same week, which would be an edge case for this analysis
-- Hence we would not be considering SKUs that have 1-to-many mapping SKU-Alerts mapping

CREATE OR REPLACE TEMP TABLE NON_DUP_ALERTS AS
SELECT
  *
FROM
  `analytics-askuity-thd.sid_workspace.SALES_ALERTS_2022`
WHERE
  SKU_NBR NOT IN (
  SELECT
    SKU_NBR
  FROM (
    SELECT
      SKU_NBR,
      WEEK,
      COUNT(DISTINCT DIFFERENCE_PERC) AS INSIGHT_VARIATION_CNT
    FROM
      `analytics-askuity-thd.sid_workspace.SALES_ALERTS_2022`
    GROUP BY
      SKU_NBR,
      WEEK
    HAVING
      INSIGHT_VARIATION_CNT>1)) ;

-- Merging the data with SKU-SALES Correlation table
-- This is done because we would like flag SKUs where the sales spike was due to promotions but 
-- the actual reason was that the SKU was on a promotion
-- Note:Query depends on the above temp table, run in the same session
CREATE OR REPLACE TABLE analytics-askuity-thd.sid_workspace.SALES_PROMO_AGG_ALERTS_CORR AS
SELECT
  SLS_PROMO_AGG.*,
  ALERT_FLAG,
  correlation AS CORRELATION
FROM
  `analytics-askuity-thd.sid_workspace.SALES_PROMO_AGG` AS SLS_PROMO_AGG
LEFT JOIN
  (
    SELECT
      SKU_NBR,
      WEEK,
      1 AS ALERT_FLAG
    FROM
      (SELECT DISTINCT SKU_NBR,WEEK  FROM NON_DUP_ALERTS) AS NON_DUP_ALERTS
  ) AS NON_DUP_ALERTS_TBL
ON(SLS_PROMO_AGG.SKU_NBR = NON_DUP_ALERTS_TBL.SKU_NBR
   AND SLS_PROMO_AGG.SLS_WEEK = NON_DUP_ALERTS_TBL.WEEK)
LEFT JOIN
  analytics-askuity-thd.sid_workspace.SALES_PROMO_2022_CORR AS CORR_TBL
ON(SLS_PROMO_AGG.SKU_NBR = CORR_TBL.SKU_NBR) ;

-- Building a table to visualize, how sales change with promotions and overlay it with alerts
CREATE OR REPLACE TABLE analytics-askuity-thd.sid_workspace.VIZ_EFFECT_PROMO_ALERTS AS
SELECT 
  A.*
  ,CASE WHEN PROMO_FLG = 1 and ALERT_FLG = 1 THEN 'PROMO ALERT'
    WHEN PROMO_FLG=1 THEN 'PROMO'
    WHEN ALERT_FLG = 1 THEN 'ALERT'
    ELSE 'NO PROMO ALERT'
  END AS PROMO_ALERT_IND,
  PROMO_FLG*TY_SLS AS PROMO_SLS,
  ALERT_FLG*TY_SLS AS ALERT_SLS
FROM(
SELECT
  SKU_NBR, 
  SLS_WEEK, 
  SUM(TY_SLS) AS TY_SLS, 
  SUM(TY_RETURNS) AS TY_RETURNS, 
  AVG(TOTAL_STORES) AS TOTAL_STORES, 
  AVG(TOTAL_STORES_W_PROMO) AS TOTAL_STORES_W_PROMO, 
  AVG(RATIO_STR_PROMO) AS RATIO_STR_PROMO, 
  AVG(ALERT_FLAG) AS ALERT_FLG, 
  MAX(PROMO_FLG) AS PROMO_FLG, 
  AVG(CORRELATION) AS CORRELATION,
FROM
  analytics-askuity-thd.sid_workspace.SALES_PROMO_AGG_ALERTS_CORR
GROUP BY
  SKU_NBR
  ,SLS_WEEK) AS A
  ;