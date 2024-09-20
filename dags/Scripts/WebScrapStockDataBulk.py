from bs4 import BeautifulSoup # type: ignore
from urllib.request import Request, urlopen
from datetime import datetime, timedelta
import pandas as pd
import http.client as http 
from http.client import IncompleteRead

import time
import random
import subprocess
import sys
import numpy as np
http.HTTPConnection._http_vsn = 11
http.HTTPConnection._http_vsn_str = 'HTTP/1.1'
request_header = {'User-Agent': 'Mozilla/5.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        }



def get_dates():
    # Get today's date and time
    today = datetime.now()
    
    # Get today's Unix timestamp
    unix_timestamp_today = int(today.timestamp())
    
    # Get the date 20 years ago
    twenty_years_ago = today - timedelta(days=20*365)
    
    # Get the Unix timestamp for the date 20 years ago
    unix_timestamp_twenty_years_ago = int(twenty_years_ago.timestamp())
    
    # Format the dates as strings
    today_str = today.strftime("%Y-%m-%d %H:%M:%S")
    twenty_years_ago_str = twenty_years_ago.strftime("%Y-%m-%d %H:%M:%S")
    
    return {
        "today_date": today_str,
        "today_unix_timestamp": unix_timestamp_today,
        "twenty_years_ago_date": twenty_years_ago_str,
        "twenty_years_ago_unix_timestamp": unix_timestamp_twenty_years_ago
    }



def get_most_active_stocks_list():
    BASE_URL_MOST_ACTIVE = "https://finance.yahoo.com/most-active/"
    res = Request(BASE_URL_MOST_ACTIVE,
        headers=request_header)
    webpage = urlopen(res).read()

    soup = BeautifulSoup(webpage, 'html.parser')

    org_symbol_list = []
    org_name_list = []
    data = soup.findAll(["table"])
    l =  data[0].select('tbody > tr > td > span > div > a > div > span')
    idx_1 , idx_2 = 0 , 1
    count = len(l)
    while idx_2 < count:
        org_symbol_list.append(l[idx_1].text)
        org_name_list.append(l[idx_2].text)        
        idx_1+=2
        idx_2+=2        
    return org_symbol_list,org_name_list

def get_stocks_data(org_symbol,start_date,end_date):
    soup = None
    try:
        req = Request('https://finance.yahoo.com/quote/'+org_symbol+'/history?period1='+start_date+'&period2='+end_date,
            headers=request_header)
        #print('https://finance.yahoo.com/quote/'+org_symbol+'/history?period1='+start_date+'&period2='+end_date)
        webpage = urlopen(req).read()

        soup = BeautifulSoup(webpage, 'html.parser')
    except IncompleteRead:
        return soup
    return soup

def get_header(soup):
    header = []
    data = soup.findAll(["table"])
    #print(data)
    for i in data[0].next_element.findAll(['th']):
        header.append(i.text[:10].strip())
    return header,data


def dump_data(data,header,org_symbol,org_name):
    import os
    #print(os.listdir('/opt/bitnami/stocks_data/'))
    with open('/opt/bitnami/stocks_data/'+org_symbol+'.csv', 'w') as file:
        for i in header:
            file.write(f"{i};")
        file.write("org_name;")
        file.write('\n')
        for i in data[0].findAll(['tr']):
            count=0
            for j in i.findAll(['td']):
                file.write(f"{j.text.strip()};")
                #print(j.text,end=";")  
                if count == 6:
                    file.write(f"{org_name};")
                    file.write('\n')
                    #print("\n")
                    count=0
                count+=1
        print("Records Count :",len(data[0].findAll(['tr'])))

def upload_sftp():
    import pysftp

    # Define the SFTP server details
    hostname = '172.18.0.7'
    port = 2222
    username = 'abhishek'
    password = 'password'
    # Define connection options to disable host key checking
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # Disable host key checking
    # Establish an SFTP connection
    with pysftp.Connection(host=hostname, username=username, password=password, port=port, cnopts=cnopts) as sftp:
        import os
        print("Connection successfully established ... ")
        for i in os.listdir("/opt/bitnami/stocks_data/"):
            file_name = '/opt/bitnami/stocks_data/'+str(i)
            target_file_name = '/config/sftp_stocks_data/'+str(i)
            sftp.put(file_name,target_file_name)
            os.remove(file_name)
        print(f"File uploaded successfully ")


def main():
    subprocess.call([sys.executable, '-m', 'pip', 'install', 'pysftp'])
    start_time =  time.time()
    dates = get_dates()
    print("Today's Date:", dates["today_date"])
    print("Today's Unix Timestamp:", dates["today_unix_timestamp"])
    print("Date 20 Years Ago:", dates["twenty_years_ago_date"])
    print("Unix Timestamp 20 Years Ago:", dates["twenty_years_ago_unix_timestamp"])

    org_symbol_list,org_name_list = get_most_active_stocks_list()
    print(org_symbol_list)
    for org_symbol,org_name in zip(org_symbol_list,org_name_list):
        print("org : ",org_symbol)
        soup = get_stocks_data(org_symbol=org_symbol,
                              start_date=str(dates["twenty_years_ago_unix_timestamp"]),
                              end_date=str(dates["today_unix_timestamp"]))
        
        if soup is None:
            pass
        else:
            header,data = get_header(soup)
            dump_data(data=data,header=header,org_symbol=org_symbol,org_name=org_name)
            
        time.sleep(random.randint(1,5))
    print("Bulk Data Fetch time taken : ",np.round(time.time()-start_time,2),' seconds')
    upload_sftp()
    print("Process Complete")


if __name__=="__main__":
    
    main()