from teamUtils import BQ_User # Pricing Analytics package we just installed
from shapely.wkt import loads

home_project='analytics-askuity-thd' # Make this the project you use in your everyday work, for example Jared would use analytics-pricing-thd
bq = BQ_User(project_name=home_project)

print("Pointer Before SQL")

sql_3 = """SELECT
*
FROM
`analytics-askuity-thd.sid_workspace.SALES_PROMO_2022_TBL`
WHERE
SKU_NBR IN (
SELECT
    DISTINCT SKU_NBR
FROM
    `analytics-askuity-thd.sid_workspace.SALES_PROMO_2022_TBL`
ORDER BY
    RAND()
LIMIT
    50000)"""

print("Pointer after SQL")

data = bq.SQL.to_df(sql_3)

print("Read data")

data.to_csv('promotions.csv', index=False)
print("Written data")

if __name__ == "__main__":
    ...
