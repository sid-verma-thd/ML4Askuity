# ## Parsing Sales alerts
#
# This script is used to parse sales data from firestore
#

# #### Importing Libraries


import json

import pandas as pd

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)


def parse_sales_alerts(df_sales_alerts: pd.DataFrame = None, source_path: str = None, dest_path: str = './parsed_sales.csv'):
    """
    Converts sales alerts data from firestore to a workable dataframe. 

    Args:
        sales_df (pd.DataFrame, optional): sales alert dataframe from Firestore
        source_path (str, optional): Path of the CSV data
        dest_path (str, optional): Path to save parsed data

    Returns:
        pd.DataFrame: Also returns the dataframe for convenience
    """
    if (df_sales_alerts and source_path) or not (df_sales_alerts or source_path):
        raise ValueError('Can only pass one of df_sales_alerts or source_path')

    if source_path:
        df_sales_alerts = pd.read_csv(source_path)
    print(df_sales_alerts.head())

    df_sales_alerts['notification_count'] = df_sales_alerts.apply(lambda row: eval(row['data'])['notification_count'],
                                                                  axis=1)

    df_sales_alerts['sales_alert'] = df_sales_alerts.apply(lambda row: eval(row['data'])['sales_alert'],
                                                           axis=1)

    # #### Parsing sales alert column

    df_sales_alerts = df_sales_alerts.explode('sales_alert')

    df_sales_alerts['sales_alert'] = [{**{'WEEK': w}, **{'USER_ID': u}, **{'NOTIFICATION_COUNT': notif_cnt}, **dc} for w, u, notif_cnt, dc in
                                      zip(df_sales_alerts['week'], df_sales_alerts['userid'], df_sales_alerts['notification_count'], df_sales_alerts['sales_alert'])]
    df_sales_alerts = pd.DataFrame(df_sales_alerts['sales_alert'].tolist())

    print(df_sales_alerts.head())

    # df_sales_alerts = df_sales_alerts.drop_duplicates()
    # df.loc[df.astype(str).drop_duplicates().index]
    df_sales_alerts = df_sales_alerts.loc[df_sales_alerts.astype(
        str).drop_duplicates().index]
    print(df_sales_alerts.head())

    df_sales_alerts.to_csv('parsed_sales.csv', index=False)

    # Testing the code
    # pd.read_csv(dest_path).head()


if __name__ == "__main__":

    parse_sales_alerts(
        source_path='sales_alerts_2022_multithread.csv', dest_path='./parsed_sales_del.csv')
