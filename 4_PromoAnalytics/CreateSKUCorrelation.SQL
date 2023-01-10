-- This table containing SKU-sales correlation with Promotions
SET
  @@dataset_project_id = 'analytics-askuity-thd';
SET
  @@dataset_id = 'sid_workspace';
  -- Very useful link for multi statement guide
  -- https://cloud.google.com/bigquery/docs/multi-statement-queries

BEGIN
CREATE OR REPLACE TABLE
SALES_PROMO_2022_CORR AS (
SELECT
  SKU_NBR,
  CORR(PROMO_FLG, TY_SLS_TOT) AS correlation
FROM (
  SELECT
    SLS_DT,
    SKU_NBR,
    STR_NBR,
    PROMO_FLG,
    AVG(TY_SLS) AS TY_SLS_TOT
  FROM
    analytics-askuity-thd.sid_workspace.SALES_PROMO_2022
  GROUP BY
    SLS_DT,
    SKU_NBR,
    STR_NBR,
    PROMO_FLG)AS A
GROUP BY
  SKU_NBR) ;
END
  ;