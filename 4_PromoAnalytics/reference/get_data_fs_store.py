"""
Download historical sales insights from  the data from FireStore. 
"""

import creds as cred
import firebase_admin
import pandas as pd
from firebase_admin import credentials, firestore


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
userid = data['USER_ID'].unique().tolist()

week_data = []
for num in range(1, 53):
    monday_week = get_mondays(num=num)
    week_data.append(monday_week)


# Looping through weeks and userids to get the data
data_sales = []
wks = []
uss = []
db = firestore.client()

for w in week_data:
    for u in userid:
        print(f'Fetching results for week {w} User {u}')
        docs = db.collection('sales_alerts_dc').document(
            u).collection(u'date').document(w).get()
        if docs.to_dict():
            # print("week",w)
            # print("user",u)
            wks.append(w)
            uss.append(u)
            data_sales.append(docs.to_dict())
            # print(f'{docs.id} => {docs.to_dict()}')

df_sales = pd.DataFrame()
df_sales['week'] = wks
df_sales['userid'] = uss
df_sales['data'] = data_sales

# Writing out the dataframe

df_sales.to_csv('sales_alerts_2022.csv', index=False)

if __name__ == "__main__":
    ...
