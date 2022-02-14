from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import datetime
import time
import pandas as pd
from sqlalchemy import create_engine
import http.client as http
http.HTTPConnection._http_vsn = 10
http.HTTPConnection._http_vsn_str = 'HTTP/1.0'

engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="root",
                               db="stocks"))

current_timestamp = int(datetime.datetime.now().timestamp())
print(current_timestamp)
org_name_url = "https://in.investing.com/equities/trending-stocks"
org_name_list = []
url = req = Request(org_name_url, headers={'User-Agent': 'Mozilla/5.0'})
webpage = urlopen(req).read()
soup = BeautifulSoup(webpage, 'html.parser')
list_of_Companyname_tags = soup.find_all(["td"], attrs={"class": "col-name"})
share_org_list = []
for i in list_of_Companyname_tags:
    share_org_list.append(i.a['href'])

"""share_org_list = ['state-bank-of-india','reliance-industries','hdfc-bank-ltd','tata-motors-ltd']"""
for org_name in share_org_list:
    print(org_name)
    start = time.time()

    url = req = Request('https://in.investing.com' + str(org_name) + '-historical-data?end_date=' + str(
        current_timestamp) + '&st_date=987100200',
        headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req).read()
    soup = BeautifulSoup(webpage, 'html.parser')
    list_of_Date_tags = soup.find_all(
        ["td", "span"], attrs={"class": "col-rowDate"})
    list_of_Open_tags = soup.find_all(
        ["td", "span"], attrs={"class": "col-last_open"})
    list_of_Close_tags = soup.find_all(
        ["td", "span"], attrs={"class": "col-last_close"})
    list_of_High_tags = soup.find_all(
        ["td", "span"], attrs={"class": "col-last_max"})
    list_of_Low_tags = soup.find_all(
        ["td", "span"], attrs={"class": "col-last_min"})
    list_of_Volume_tags = soup.find_all(
        ["td", "span"], attrs={"class": "col-volume"})
    list_of_percentageChng_tags = soup.find_all(
        ["td", "span"], attrs={"class": "col-change_percent"})

    end = time.time()
    data = []
    for date, open, close, high, low, volume, percentageChange in zip(list_of_Date_tags, list_of_Open_tags,
                                                                      list_of_Close_tags, list_of_High_tags,
                                                                      list_of_Low_tags, list_of_Volume_tags,
                                                                      list_of_percentageChng_tags):
        data.append([date.contents[1].contents[0], open.contents[1].contents[0], close.contents[1].contents[0],
                     high.contents[1].contents[0],
                     low.contents[1].contents[0], volume.contents[1].contents[0],
                     percentageChange.contents[1].contents[0]])
    print(end," : ",start)
    print(end - start, " seconds")
    df = pd.DataFrame(data, columns=[
                      'date', 'open', 'close', 'high', 'low', 'volume', 'percentageChange'])
    vol=[]
    for i in df.volume:
        i=i.replace(",","")
        if "K" in i:
            vol.append(float(i.replace("K",""))*1000)
        elif "M" in i:
            vol.append(float(i.replace("M",""))*1000000)
        elif "B" in i:
            vol.append(float(i.replace("B",""))*1000000000)
        else:
            vol.append(float(i))
    df['volume'] = vol
    df['percentageChange'] =df['percentageChange'].str.replace("%","")
    df['open'] =df['open'].str.replace(",","")
    df['close'] =df['close'].str.replace(",","")
    df['high'] =df['high'].str.replace(",","")
    df['low'] =df['low'].str.replace(",","")

    try:
        print(df.head())
        df.to_csv("." + org_name + ".csv", index=None)
        df.to_sql(org_name, con = engine, if_exists = 'append')
        print("data saved for org_name : ", org_name)
    except:
        continue
