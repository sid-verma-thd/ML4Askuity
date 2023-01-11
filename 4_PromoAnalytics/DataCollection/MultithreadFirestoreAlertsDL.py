"""
This is an example of multithreading to fetch data from Firestore.
Speeds up the code by 12x
"""

import concurrent.futures
import time

import creds as cred
import firebase_admin
import pandas as pd
from firebase_admin import credentials, firestore

start = time.perf_counter()


# Helper functions
def get_mondays(num):
    import datetime
    today = datetime.datetime.now()
    week = today - datetime.timedelta(weeks=num)

    sundays = week - \
        datetime.timedelta(days=week.weekday()) - datetime.timedelta(days=1)

    return sundays.date().strftime('%Y-%m-%d')


def fs_data(db, collection, user, week):
    return collection(collection).document(week).collection(user).stream()


# Authenticating and creating a credentials object
# Connects with firestore and fetch data
project_name = "rc-ask-askuity"

cred = credentials.Certificate("creds/rc-ask-askuity.json")
firebase_admin.initialize_app(cred, {
    'projectId': project_name,
})

# We would need the userids and weeks to get the data
data = pd.read_csv('UserId_2022.csv')
userids = data['USER_ID'].unique().tolist()


week_data = []
for num in range(1, 53):
    monday_week = get_mondays(num=num)
    week_data.append(monday_week)

# Creating a user,week mapping
comb_user_week = [(user, week) for user in userids for week in week_data]


data_sales = []
wks = []
uss = []
db = firestore.client()


def download_sales(comb_user_week):
    (userid, week) = comb_user_week
    print(f'Fetching results for week {week} User {userid}')
    docs = db.collection('sales_alerts_dc').document(
        userid).collection(u'date').document(week).get()
    if docs.to_dict():
        wks.append(week)
        uss.append(userid)
        data_sales.append(docs.to_dict())


with concurrent.futures.ThreadPoolExecutor() as executor:
    # comb_user_week = comb_user_week[0:10] # Was used for testing
    args = comb_user_week
    executor.map(download_sales, args)


df_sales = pd.DataFrame()
df_sales['week'] = wks
df_sales['userid'] = uss
df_sales['data'] = data_sales

# Writing out the dataframe

df_sales.to_csv('sales_alerts_2022_multithread.csv', index=False)
