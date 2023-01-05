-- Below is the view that powers a Looker Studio Dashboard
SELECT
  sku.week
  ,sku.week_index
  ,sku.product_key
  ,sku_str.store_key
  ,sku.sales_units as sales_units_sku
  ,sku_str.sales_units as sales_units_skustr
  ,sku.diff_amount as diff_amount_sku
  ,sku_str.diff_amount as diff_amount_skustr
  ,case when sum(sku_str.diff_amount) over (partition by week,week_index, product_key) !=0 then
  sku_str.diff_amount*100/(sum(sku_str.diff_amount) over (partition by week,week_index, product_key)) end as pct_str_lvl_ctb
  ,sku.consecutive_count
FROM
  `analytics-askuity-thd.notification_salesunits_skustore.NOTIFICATION_SKU` as sku
JOIN
  `analytics-askuity-thd.notification_salesunits_skustore.NOTIFICATION_SKU_STR` as sku_str 
  USING (week, week_index, product_key)
WHERE
  sku.consecutive_count >= 2 AND
  sku.increase = sku_str.increase