-- Quality Checks as we build the data
-- Checking SALES_PROMO TABLE DATA ROWS
SELECT((
SELECT 
  COUNT(*)
FROM
  analytics-askuity-thd.sid_workspace.SALES_PROMO_2022
WHERE  
  sku_nbr=1007727797
  AND sls_dt>'2022-01-01') 
  ,(
SELECT 
  COUNT(*)
FROM
  analytics-askuity-thd.sid_workspace.SALES_PROMO_2022_TBL
WHERE  
  sku_nbr=1007727797
  AND sls_dt>'2022-01-01') 
,(SELECT 
  COUNT(*)
FROM
  pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SALES_v2
WHERE  
  sku_nbr=1007727797
  AND
      DATE(SLS_DT)>'2022-01-01'
      AND ((TY_SLS IS NOT NULL)-- conditions to remove rows where both TY_SLS AND RETURNS ARE 0 or NULL
        OR (TY_RETURNS IS NOT NULL))
      AND ((TY_RETURNS != 0)
        OR (TY_SLS IS NOT NULL))))
;

SELECT 
  *
FROM
  analytics-askuity-thd.sid_workspace.SALES_PROMO_2022
WHERE  
  sku_nbr=1007727797
  AND sls_dt>'2022-01-01'
LIMIT 1000
;

--- QA SALES PROMO AGG
SELECT((
SELECT 
  COUNT(*)
FROM
  analytics-askuity-thd.sid_workspace.SALES_PROMO_2022
WHERE  
  sku_nbr=1007727797
  AND sls_dt>'2022-01-01') 
  ,(
SELECT 
  COUNT(*)
FROM
  analytics-askuity-thd.sid_workspace.SALES_PROMO_AGG
WHERE  
  sku_nbr=1007727797
  AND sls_dt>'2022-01-01') ) ;


-- QA SALES_PROMO_AGG_ALERTS_CORR

SELECT * FROM analytics-askuity-thd.sid_workspace.SALES_PROMO_2022_CORR LIMIT 10;

SELECT((
SELECT 
  COUNT(*)
FROM
  analytics-askuity-thd.sid_workspace.SALES_PROMO_2022
WHERE  
  sku_nbr=1007727797
  AND sls_dt>'2022-01-01') 
  ,(
SELECT 
  COUNT(*)
FROM
  analytics-askuity-thd.sid_workspace.SALES_PROMO_AGG_ALERTS_CORR
WHERE  
  sku_nbr=1007727797
  AND sls_dt>'2022-01-01') ) ;

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

SELECT
  SKU_NBR,
  WEEK,
  COUNT(*) AS COUNT_ROWS
FROM
(SELECT DISTINCT SKU_NBR,WEEK FROM NON_DUP_ALERTS) AS NON_DUP_ALERTS
GROUP BY
  1,2
ORDER BY
COUNT_ROWS DESC ;

-- TESTING VIZ TABLE
SELECT * FROM analytics-askuity-thd.sid_workspace.VIZ_EFFECT_PROMO_ALERTS where alert_sls is not null LIMIT 100 ;

SELECT
  SKU_NBR
  ,PROMO_FLG
  ,COUNT(DISTINCT SLS_WEEK)
FROM
  analytics-askuity-thd.sid_workspace.VIZ_EFFECT_PROMO_ALERTS
GROUP BY
  1,2 ;

-- QA SALES NUMBERS

SELECT 
  SUM(TY_SLS) 
FROM 
  pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SALES_v2
WHERE 
  SKU_NBR = 219823
  AND sls_dt>'2022-01-01' ;

