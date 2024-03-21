from pandas import *
import sys
import glob
import os
import gzip
import json
from datetime import date, timedelta, datetime
from dateutil import parser
import time
import requests

data_path = sys.argv[1]
datetime_range_start = sys.argv[2] #"YYYY-MM-DD hh:mm:ss"
datetime_range_end = sys.argv[3] #"YYYY-MM-DD hh:mm:ss"
URL = sys.argv[4]

url_filter = "20.200.219.101"

# data = read_csv("allTTSwebhooktest.csv")
# url_filter = "eoy"





def date_range_(start_dt, end_dt):
    dates = []
    delta = timedelta(days=1)
    while start_dt <= end_dt:
        # add current date to list by converting  it to iso format
        dates.append(start_dt.isoformat())
        # increment start date by timedelta

        start_dt += delta
    return dates
    

def time_in_range(start, end, x):
    """Return true if x is in the range [start, end]"""
    if start <= end:
        return start <= x <= end
    else:
        return start <= x or x <= end

def process(content, file_name):
    payload = json.loads(content)
    received_at = parser.parse(payload["received_at"].split(".")[0] + "Z").replace(tzinfo=None)
    #print(start_date, received_at)
    if time_in_range(start_date, end_date, received_at):

        res = requests.post(URL, json=json.loads(content))
        #time.sleep(0.1)
        print("res", res)

            
            


start_date = datetime.strptime(datetime_range_start, "%Y-%m-%d %H:%M:%S")
end_date = datetime.strptime(datetime_range_end, "%Y-%m-%d %H:%M:%S")

for dt in date_range_(start_date, end_date):
    dir_path = f'{data_path}/*{dt.split("T")[0]}*'
    sorted_path = sorted(glob.glob(dir_path))
    #print(sorted_path)
    
    for path in sorted_path:
        if os.path.isfile(path):
            try:
                with gzip.open(f'{path}',mode='r') as thefile:
                    count = 0
                    for i in thefile:
                        count += 1
                        content = str(i.decode("utf-8-sig"))
                        
                        process(content, path)
                        #break
                    print(f"NO MORE CONTENT: {count}")
                    #break
                    
            except Exception as e:
                 print(e)