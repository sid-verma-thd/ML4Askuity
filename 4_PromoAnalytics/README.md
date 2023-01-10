# Promotional Analytics

**Situation:**

Our current solution picks up sales anomalies in data based on the magnitude of difference between current sales and expected sales

**Complication**

We would like to explain some of the reasons why we see sales anomalies. 

**Question**

The main question we are trying to answer is what's the impact of promotions on Alerts and Sales spikes.

# Steps Followed to Build Analysis

**1. Understand Promotions Table**
- Sales `pr-edw-views-thd.VENDOR_COLLAB_VIEWS.SALES_v2` and Promotions `pr-edw-views-thd.SHARED_COMMONS.SKU_STR_RETL_PROMO` table was joined to create `analytics-askuity-thd.sid_workspace.SALES_PROMO_2022` . Reference query to build the data is `CreateSalesPromo.SQL`
- Analysis was built on looker studio [Dashboard](https://datastudio.google.com/u/0/reporting/69f327c1-22a4-4164-b0c1-d8895bfde7e2/page/tEnnC/edit)
- A hypothesis was established that there are certain SKUs where the sales vary with promotions `PromoAnalysis.ipynb` data for which was downloaded using the script `GetPromotionsBQ.py`

**2. Build SALES-PROMOTION correlation table for each SKU** <br>

Not every SKU sales are correlated to promotions. Some of the cheaper SKU Sales dont really depend on promotions. Hence it was important to create a SKU-Sales to Promotion correlation table to understand which SKUs sales are sensitive to promotions

- SKU Sales to Promotions table was created using `CreateSKUCorrelation.SQL`

**3. Get Alerts data from Firestore**
- We need userId to fetch sales alerts data from firestore, which can be generated using `GetActiveUserIdForFirestore.SQL`
- We used `MultithreadFirestoreAlertsDL.py` to download data from Firestore. <br>
Note: Multithreading was used due to high latency between API calls to fetch data

- Alerts are downloaded in JsonLike format, which is harder than usual to parse. `ParsingAlertsFirestore.py` was used to parse raw alerts data from Firestore

**4. Build SALES-PROMOTION-ALERTS-CORR table**

- A single source of truth with sales, promotion, alerts and Sales-Promo Correlation was built using `CreateSalesAlertsPromoCorrAgg.SQL` 
- QA can be followed in the script `QADataGeneration.SQL`

**5. Build Table for Visualizing Results**

- A final table for visualization was created using later half of the script `CreateSalesAlertsPromoCorrAgg.SQL` 

- Final Dashboard was created for reference here: [Dashboard](https://datastudio.google.com/u/0/reporting/6df19b43-9805-4c91-9694-50accb313e4a/page/ZIDCD/edit)


**Result** <br> 
This experiment shows promise to improve explainability of Sales Insights 



