import os
from tracemalloc import start 
import numpy as np
import pandas as pd # type: ignore
from datetime import date
from sqlalchemy import create_engine , text
from mysql_helper_funtions import get_current_end_date, get_stock_mstr_key, insert_new_stock, update_stock_end_date
import time
def parse_date(date_str):
    try:
        return pd.to_datetime(date_str, format='%B %d, %Y')
    except ValueError:
        return pd.to_datetime(date_str)
def convert_to_number(num_str):
    if num_str == '-':
        return 0
    try:
        return pd.to_numeric(num_str.replace(',', ''))
    except ValueError:
        return pd.to_numeric(num_str)

if __name__ == "__main__":
    start_time = time.time()
    file_list = os.listdir("./stocks_data")
    engine = create_engine("mysql+mysqlconnector://root:root@localhost:3307")
    with engine.connect() as connection :
        for i in file_list:
            stock_is_new = None
            stock_name = i.replace(".csv",'')
            temp =  connection.execute(text("SELECT * FROM stocks.stocks_master s WHERE s.stock_name = '"+stock_name+"';"))
            data = pd.read_csv("./stocks_data/"+i,delimiter=';',on_bad_lines="skip")
            data['Date'] = pd.to_datetime(data['Date'], format="%b %d, %Y").dt.date
            if len(temp.fetchall()) == 0:
                print(stock_name+" doesn't_exist in Stock Master")
                start_date = data['Date'].min() 
                end_date = data['Date'].max()
                org_name = data['org_name'][0]
                insert_new_stock(stock_name=stock_name,org_name=org_name,start_date=start_date, end_date=end_date,last_updated=date.today())
                stock_is_new = 1
            else:
                print(stock_name+ " exists in stock master")
                stock_is_new = 0
            stock_mstr_key = get_stock_mstr_key(stock_name=stock_name)
            print(stock_name,'-',stock_mstr_key)
            
            data = data[['Date','Open','High','Low','Close','Adj Close','Volume']]
            data['stock_mstr_key'] = stock_mstr_key
            data['Volume'] = data['Volume'].apply(convert_to_number)

            if stock_is_new == 1:
                data.to_sql('stock_data', con=engine,schema='stocks', if_exists='append', index=False)
            else:
                end_date = get_current_end_date(stock_name)
                data = data[data['Date'] > end_date]
                if data.shape[0]==0:
                    print("No records to insert in Stock Data up-to date")
                    
                else:
                    end_date = data['Date'].max()
                    update_stock_end_date(stock_name=stock_name, end_date=end_date) 
                    data.to_sql('stock_data', con=engine,schema='stocks', if_exists='append', index=False)
                    print("Data Updated")
            os.remove("./stocks_data/"+i)
            #print("file name : ",i)
        print("Data Load time taken : ",np.round(time.time()-start_time,2),' seconds')
                    
                
                