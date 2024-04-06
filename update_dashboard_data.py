
# THIS FILE BELONGS TO DASHBOARD MODULE

# import datetime
# import time
import requests
import schedule 
import os

# for loading the dashboard data
def run_job():

    # URL format: "http://0.0.0.0:0/graph/get_data"
    url = os.environ.get("DASH_URL")
    headers = {'Content-Type': 'application/json'}
    data = ''
    response = requests.post(url=url,headers=headers,data=data)
    if response.status_code == 200:
        display_data = "data loaded successfully"
        print(display_data)
    else:
        display_data = "data loading failed"
        print(display_data)

# Schedule the file to run on every day at desired time
schedule.every().day.at("08:18:14").do(run_job)
while True:
    schedule.run_pending()