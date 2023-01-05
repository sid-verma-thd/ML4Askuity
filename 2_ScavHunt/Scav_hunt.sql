------------------
-- 1, 2, 3 confirm access to bigquery, analytics-askuity-thd, pr-edw-views-thd, VENDOR_COLLAB_VIEWS, SHARED_COMMONS
------------------ 
  
SELECT
  *
FROM
  pr-edw-views-thd.VENDOR_COLLAB_VIEWS.INVENTORY_DAILY
LIMIT
  10

------------------
-- 4, 5. Identify Primary Inventory Metric and Sales Metric tables
------------------ 
-- Inventory daily metric

SELECT
  *
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.INVENTORY_DAILY_v2`
LIMIT
  1000

  -- Inventory weekly metrics
SELECT
  *
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.INVENTORY_DAILY_v2`
LIMIT
  1000

-- Sales Metrics

SELECT
  *
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SALES_v2`
LIMIT
  100
------------------
-- 6. Identify the lowest level temporal aggregation (i.e. daily or weekly) for the sales and inventory tables
------------------

-- Sales table
SELECT
  DISTINCT(SLS_DT) AS distinct_dt
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SALES_v2`
WHERE
  SLS_DT>'2022-10-01'
ORDER BY
  distinct_dt

-- Inventory table
SELECT
  DISTINCT(CAL_DT) AS distinct_dt
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.INVENTORY_DAILY_v2`
ORDER BY
  distinct_dt

-- Looks like the lowest level could be at a daily level.

------------------
-- 7. Identify the table required to identify the SKU hierarchy
------------------

SELECT
  *
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SKU_HIER`
LIMIT
  1000

------------------
-- 8. Identify the table required to identify the STR hierarchy
------------------

SELECT
  *
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.STR_HIER`
LIMIT
  1000

------------------
-- 10. What are all the levels in the SKU hierarchy? (Query)
------------------
-- Distinct hierarchies are SKU, SUB_SC(sub sub class), SC(Subclass), CTG(Category), CLASS, MER_DEPT, DEPT

SELECT
  sku_nbr,
  SUB_SC_NBR,
  SUB_CLASS_NBR,
  CTG_NBR,
  CLASS_NBR,
  MER_DEPT_NBR,
  DEPT_NBR
FROM
  pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SKU_HIER

  ------------------
-- 11. What are all the levels in the STR hierarchy? (Query)
------------------
-- Distinct hierarchies are STR, MKT(sub sub class), SC(Subclass), CTG(Category), CLASS, MER_DEPT, DEPT, DST(District), RGN(Region), DIV(Division)

SELECT
  str_nbr,
  mkt_NBR,
  DST_NBR,
  RGN_NBR,
  DIV_NBR
FROM
  pr-edw-views-thd.VENDOR_COLLAB_VIEWS.STR_HIER

------------------
-- 12. Which Market has the most stores in it? (Query)
------------------
SELECT
  MKT_NBR
  ,MKT_NM
  ,COUNT(DISTINCT STR_NBR) AS COUNT_STR_BY_MKT
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.STR_HIER`
GROUP BY
  MKT_NBR
  ,MKT_NM
ORDER BY
  COUNT_STR_BY_MKT DESC

-- 0048-LA MARKET has most stores

------------------
-- 13. Which Market does Atlanta fall in? (Query)
------------------
SELECT 
  MKT_NBR
  ,MKT_NM 
FROM 
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.STR_HIER` WHERE 
  CITY_NM='ATLANTA'
-- ATLANTA falls in 0001 mkt_nbr and has 0001-ATLANTA mkt_nm

------------------
-- 13. What is the field identifies a sub-vendor?  
------------------
--MVNDR IDENTIFIES SUB-VENDOR PROBABLY
-- BELOW IS THE COUNT OF DISTINCT SUBVENDORS FOR ALL VENDORS

SELECT
  PVNDR_NBR,
  COUNT(DISTINCT MVNDR_NBR) AS DISTINCT_VNDR
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.VENDOR_USER_ID`
GROUP BY
  1
ORDER BY
  DISTINCT_VNDR DESC

------------------
-- 14. What are all the vendor mapping levels?  (Query)  
------------------
SELECT
  PVNDR_NBR
  ,MVNDR_NBR
FROM 
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.MVNDR_HIER`

------------------
-- 15. How many unique MVNDRs currently sell at The Home Depot US?  (Query) 
------------------
SELECT
  COUNT(DISTINCT MVNDR_NBR)
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.MVNDR_HIER`

------------------
-- 16. Which PVNDR has the most MVNDRs as per MVNDR_HIER? How many MVNDRs does that PVNDR have?  (Query)
------------------
SELECT
  PVNDR_NBR
  ,PVNDR_NM
  ,count(distinct MVNDR_NBR) DISTINCT_MVNDR
FROM 
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.MVNDR_HIER`
group by PVNDR_NBR,PVNDR_NM
order by DISTINCT_MVNDR DESC
LIMIT 1

------------------
-- 17. We can map user to vendor by updating using the below table?  (Query)
------------------
SELECT
  *
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.VENDOR_USER_ID`
LIMIT
  1000
-- Not sure if we update or if we get these mappings from dataconnect as well

------------------
-- 18. How to you map a user to the overall vendor company?  (Query)
------------------
-- Something on these lines

-- UPDATE `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.VENDOR_USER_ID`
-- SET USER_ID = 'required_id',
-- WHERE PVNDR_NBR=1234

------------------
-- 19. What are the fields required to uniquely identify a SKU? 
------------------
-- We just need the SKU_NBR to identify unique SKU

------------------
-- 20. How many unique SKU's have had at least one sale this year?  (Query)
------------------
SELECT
  *
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SALES_v2`
WHERE
  SLS_DT>='2022-01-01'
  AND TY_SLS_UNITS>0


------------------
-- 21. What are the fields required to uniquely map a SKU to a vendor?
------------------

-- pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SKU_HIER has a mapping between SKU and Vendor

------------------
-- 22. Is a SKU always fixed to the same product? If not, how do we identify if a SKU definition has changed?
------------------

-- There are more than one SKU for every SKU DESC
SELECT
  SKU_DESC,
  COUNT(DISTINCT sku_nbr) AS DISTINCT_CNT_SKU
FROM
  `pr-edw-views-thd.SHARED_COMMONS.SKU_HIER`
GROUP BY
  SKU_DESC


-- Maybe use the most recent SKU for every SKU_NBR
SELECT
  SKU_NBR,
  SKU_DESC,
  RANK() OVER(PARTITION BY SKU_DESC ORDER BY SKU_CRT_DT DESC) AS RNK_SKU
FROM
  `pr-edw-views-thd.SHARED_COMMONS.SKU` QUALIFY RNK_SKU=1

------------------
-- 23. How do we identify all of the sales channels of a SKU?  (Query)
------------------  
SELECT
  DISTINCT SKU_NBR,
  SELLING_CHNL
FROM
  `pr-edw-views-thd.SHARED_COMMONS.SKU_STR_RETL_ASSRT`


------------------
-- 24. How do we identify all of the sales channels of a SKU?  (Query)
------------------  

SELECT
  DISTINCT SKU_NBR,
  SELLING_CHNL
FROM
  `pr-edw-views-thd.SHARED_COMMONS.SKU_STR_RETL_ASSRT`
WHERE
  SKU_NBR=1002923964


------------------
-- 25. How do we identify the current status of a SKU? (i.e. active, seasonal)
------------------  
SELECT
  DISTINCT SKU_CRT_DT,
  SKU_NBR,
  STRATEGY_TYP
FROM
  `pr-edw-views-thd.SHARED_COMMONS.SKU_STR_RETL_ASSRT`
WHERE
  SKU_NBR=1002923964

------------------
-- 26. How many active SKUs are currently there
------------------  
SELECT
  DISTINCT *
FROM
  `pr-edw-views-thd.SHARED_COMMONS.SKU_STR_RETL_ASSRT`
WHERE
  STRATEGY_TYP='ACTIVE'
  AND DATE(LAST_UPD_TS)='2022-12-13'

------------------
-- 27. How do we identify the vendors currently selling a SKU?
------------------  

SELECT
  DISTINCT SKU_NBR,
  STR_NBR,
  MVNDR_NBR,
  DEPT_NBR
FROM
  `pr-edw-views-thd.SHARED_COMMONS.SKU_STR_RETL`
WHERE
  DATE(LAST_UPD_TS)='2022-12-13'


------------------
-- 28. How we identify the current price of a SKU?
------------------  

SELECT
  SKU_NBR,
  STR_NBR,
  EFF_RETL_AMT
FROM
  `pr-edw-views-thd.SHARED_COMMONS.SKU_STR_RETL_ASSRT`
WHERE
  STRATEGY_TYP='ACTIVE'
  AND DATE(LAST_UPD_TS)='2022-12-13'


------------------
-- 29. What are the top 10  the most expensive selling at the moment?  (Query)
------------------ 
-- ASSUMPTION - IF THE SKU SELLS AT 2 STORES AT DIFFERENT PRICES, THE HIGHER PRICE IS CONSIDERED

SELECT 
  *
FROM
(
  SELECT DISTINCT
    SKU_NBR,
    EFF_RETL_AMT,
    RANK() OVER (PARTITION BY SKU_NBR ORDER BY EFF_RETL_AMT DESC) AS HIGHEST_PRICE_SKU
  FROM
    `pr-edw-views-thd.SHARED_COMMONS.SKU_STR_RETL_ASSRT`
  WHERE
    STRATEGY_TYP='ACTIVE'
    AND DATE(LAST_UPD_TS)='2022-12-13'
  QUALIFY HIGHEST_PRICE_SKU=1) AS TEMP
ORDER BY EFF_RETL_AMT DESC
LIMIT 10

------------------
-- 30. What is the current MSRP of a SKU?  (Query)
------------------ 

-- Filter for a particular SKU

SELECT
    DISTINCT SKU_NBR,
    STR_NBR,
    MSRP_AMT,
    EFF_BGN_DT,
    EFF_END_DT
  FROM
    `pr-edw-views-thd.SHARED_COMMONS.SKU_STR_RETL`
  WHERE
    DATE(LAST_UPD_TS)=CURRENT_DATE()
    AND MSRP_AMT IS NOT NULL
    -- AND MSRP_AMT !=0
    AND DATE(EFF_END_DT)>=CURRENT_DATE()
    AND DATE(EFF_BGN_DT)<=CURRENT_DATE()
  ORDER BY SKU_NBR, STR_NBR

-- QC - We dont have multiple MSRPs at SKU-STR combination

SELECT
  SKU_NBR
  ,STR_NBR
  ,count(*) as unique_combination_counts
FROM(
  SELECT
    DISTINCT SKU_NBR,
    STR_NBR,
    MSRP_AMT,
    EFF_BGN_DT,
    EFF_END_DT
  FROM
    `pr-edw-views-thd.SHARED_COMMONS.SKU_STR_RETL`
  WHERE
    DATE(LAST_UPD_TS)=CURRENT_DATE()
    AND MSRP_AMT IS NOT NULL
    -- AND MSRP_AMT !=0
    AND DATE(EFF_END_DT)>=CURRENT_DATE()
    AND DATE(EFF_BGN_DT)<=CURRENT_DATE()
  ORDER BY SKU_NBR, STR_NBR) as temp
GROUP BY SKU_NBR, STR_NBR
ORDER BY unique_combination_counts DESC


------------------
-- 31. Is the sale price of a SKU standard nation wide or is it regionally priced
------------------ 

-- It's priced different, there are variations in the price for the same SKU at a store level


------------------
-- 32. What is the SKU with the highest sales variability across stores?
------------------ 
SELECT
  DISTINCT
  SKU_NBR,
  VARIANCE( TY_SLS_UNITS )  OVER (PARTITION BY SKU_NBR) as VAR_SKU
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SALES_v2`
WHERE
  sls_dt>'2022-01-01'
ORDER BY VAR_SKU DESC
LIMIT 10

------------------
-- 33. How do we identify if a SKU is currently in promotion status?
------------------ 
-- Add a where clause for the SKU you wish to find is promoted of not
SELECT
  *
FROM
  `pr-edw-views-thd.SHARED_COMMONS.SKU_STR_RETL_ASSRT`
WHERE
  DATE(RUN_TS)='2022-12-13'
  AND ASSRT_FLG=TRUE
  AND PROMO_FLG=TRUE
  AND DATE(EFF_END_DT)>=CURRENT_DATE()
  AND DATE(EFF_BGN_DT)<=CURRENT_DATE()

------------------
-- 34. How many SKU's have had a promotion this year?  (Query)
------------------ 
SELECT
  COUNT(DISTINCT SKU_NBR)
FROM
  `pr-edw-views-thd.SHARED_COMMONS.SKU_STR_RETL_ASSRT`
WHERE
  DATE(RUN_TS)='2022-12-13'
  AND ASSRT_FLG=TRUE
  AND PROMO_FLG=TRUE
  AND DATE(EFF_END_DT)>=CURRENT_DATE()
  AND DATE(EFF_BGN_DT)<=CURRENT_DATE()


------------------
-- 35. How do we identify the name, type, description and sale price of sales promotion?
------------------ 

SELECT
  STRATEGY_TYP AS NAME
  ,STRATEGY AS STRATEGY_TYPE
  ,SKU_NBR
  ,STR_NBR
  ,EFF_RETL_AMT AS SALES_PRICE
  ,PREV_PERM_RETL_AMT AS PREVIOUS_PRICE
  ,EFF_BGN_DT
  ,EFF_END_DT
FROM
  `pr-edw-views-thd.SHARED_COMMONS.SKU_STR_RETL_ASSRT`
WHERE
  DATE(RUN_TS)=CURRENT_DATE()
  AND ASSRT_FLG=TRUE
  AND PROMO_FLG=TRUE
  AND DATE(EFF_END_DT)>=CURRENT_DATE()
  AND DATE(EFF_BGN_DT)<=CURRENT_DATE()


------------------
-- 36. What are the total GROSS sales in the past month?  (Query)
------------------ 

SELECT
  SUM(TY_SLS) AS NET_SALES,
  SUM((TY_SLS - TY_RETURNS)) AS GROSS_SALES
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SALES_v2`
WHERE
  EXTRACT(MONTH FROM sls_dt)= EXTRACT(MONTH FROM CURRENT_DATE())-1


------------------
-- 37. What are the total NET sales in the past month?  (Query)
------------------ 

SELECT
  SUM(TY_SLS) AS NET_SALES,
  SUM((TY_SLS - TY_RETURNS)) AS GROSS_SALES
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SALES_v2`
WHERE
  EXTRACT(MONTH FROM sls_dt)= EXTRACT(MONTH FROM CURRENT_DATE())-1


------------------
-- 38. What explains the difference between gross and net sales amounts?  (Query)
------------------ 

-- The difference between gross and net sales can be explained by the returns that happened in the last month

------------------
-- 39. Identify the top 10 PVNDRs based on sales in 2020. (Query)
------------------ 

-- ADD A QUALIFY <=10 statement for top 10 PVNDRs

WITH SALES_2020 AS
(SELECT
  MVNDR_NBR,
  SUM(TY_SLS) AS NET_SALES,
  SUM((TY_SLS - TY_RETURNS)) AS GROSS_SALES
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SALES_v2`
WHERE
  EXTRACT(YEAR FROM sls_dt)= 2020
GROUP BY
  MVNDR_NBR
  )
, MVNDR_HIER AS
(SELECT
    DISTINCT PVNDR_NBR,
    PVNDR_NM,
    MVNDR_NBR,
    MVNDR_NM
  FROM
    `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.MVNDR_HIER`)
SELECT 
  M.PVNDR_NBR
  ,M.PVNDR_NM
  ,SUM(NET_SALES) AS NET_SALES_TOT
  ,SUM(GROSS_SALES) AS GROSS_SALES_TOT
  ,RANK() OVER (ORDER BY SUM(NET_SALES) DESC) AS RANK_SALES
FROM
  SALES_2020 as S
LEFT JOIN
  MVNDR_HIER as M
USING(MVNDR_NBR)
GROUP BY
  PVNDR_NBR
  ,PVNDR_NM
ORDER BY RANK_SALES

------------------
-- 40. Identify the top 10 products based on YoY comp during COVID (Feb/20 - Nov/20)  (Query)
------------------ 
-- Can do a limit 10 if we wish to only look at the top 10 products

SELECT
  SKU_NBR,
  SUM(TY_SLS_UNITS) AS THIS_YEAR_SALES,
  SUM(LY_SLS_UNITS) AS LAST_YEAR_SALES,
  (SUM(TY_SLS_UNITS)-SUM(LY_SLS_UNITS)) AS SALES_INCREMENT,
  CASE
    WHEN SUM(LY_SLS_UNITS)=0 THEN 0
  ELSE
  (SUM(TY_SLS_UNITS)-SUM(LY_SLS_UNITS))/SUM(LY_SLS_UNITS)
END
  AS PCT_SALES_INCREMENT
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SALES_v2`
WHERE
  sls_dt>='2020-02-01'
  AND sls_dt<='2020-11-30'
GROUP BY
  SKU_NBR
ORDER BY
  PCT_SALES_INCREMENT DESC

------------------
-- 41. Which categories have had the highest sales increases based on YoY comp during COVID (Feb/20 - Nov/20)  (Query)
------------------ 


--Helper queries, trying to find unique SKU-Category mapping
SELECT
  SKU_NBR,
  COUNT(DISTINCT CTG_NBR) AS COUNT_DISTINCT_CTG_NBR
FROM(
  SELECT
    SKU_NBR,
    CTG_NBR,
    RANK() OVER (PARTITION BY SKU_NBR ORDER BY SKU_CRT_DT DESC) as LATEST_CAT,
  FROM
    `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SKU_HIER`
  QUALIFY LATEST_CAT=1) as temp
GROUP BY 1
ORDER BY COUNT_DISTINCT_CTG_NBR DESC

--Main Query

WITH
  SKU_CAT_MAP AS (
  SELECT
    SKU_NBR,
    CTG_NBR,
    CTG_DESC
  FROM (
    SELECT
      SKU_NBR,
      CTG_NBR,
      CTG_DESC,
      RANK() OVER (PARTITION BY SKU_NBR ORDER BY SKU_CRT_DT DESC) AS LATEST_CAT,
    FROM
      `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SKU_HIER` QUALIFY LATEST_CAT=1) AS TEMP),
  SALES AS (
  SELECT
    SKU_NBR,
    TY_SLS,
    LY_SLS,
  FROM
    `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SALES_v2`
  WHERE
    sls_dt>='2020-02-01'
    AND sls_dt<='2020-11-30')
SELECT
  SKU.CTG_NBR,
  SKU.CTG_DESC,
  SUM(SALES.TY_SLS) AS TOT_THIS_YEAR_SALES,
  SUM(SALES.LY_SLS) AS TOT_LAST_YEAR_SALES,
  CASE
    WHEN SUM(LY_SLS)=0 THEN 0
  ELSE
  (SUM(TY_SLS)-SUM(LY_SLS))/SUM(LY_SLS)
END
  AS PCT_SALES_INCREMENT
FROM
  SALES
LEFT JOIN
  SKU_CAT_MAP AS SKU
USING
  (SKU_NBR)
GROUP BY
  CTG_NBR,
  CTG_DESC
ORDER BY
  PCT_SALES_INCREMENT DESC
  

------------------
-- 42. Which markets have highest sales increases based on YoY comp during COVID (Feb/20 - Nov/20)  (Query)
------------------ 

SELECT
  STR.MKT_NBR,
  STR.MKT_NM,
  SUM(SALES.TY_SLS) AS TOT_THIS_YEAR_SALES,
  SUM(SALES.LY_SLS) AS TOT_LAST_YEAR_SALES,
  CASE
    WHEN SUM(LY_SLS)=0 THEN 0
  ELSE
  (SUM(TY_SLS)-SUM(LY_SLS))/SUM(LY_SLS)
END
  AS PCT_SALES_INCREMENT
FROM
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SALES_v2` AS SALES
LEFT JOIN
  `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.STR_HIER` AS STR
USING
  (STR_NBR)
WHERE
    sls_dt>='2020-02-01'
    AND sls_dt<='2020-11-30'
GROUP BY
  MKT_NBR
  ,MKT_NM
ORDER BY
  PCT_SALES_INCREMENT DESC

------------------
-- 43. Which products have the highest ratio of on-order to on-hand units in the past year?  (Query)
------------------ 

SELECT
  SKU_NBR
  ,AVG(MEAN_OO_OH) AS MEAN_OO_OH
FROM
  (SELECT
    SKU_NBR,
    CASE WHEN TY_DLY_OH_QTY!=0 THEN
      TY_DLY_OO_QTY/TY_DLY_OH_QTY END AS MEAN_OO_OH
  FROM
    `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.INVENTORY_DAILY_v2`
  WHERE
    EXTRACT(YEAR
    FROM
      CAL_DT)= 2022
  ) as temp
GROUP BY
  SKU_NBR
ORDER BY
  MEAN_OO_OH DESC

