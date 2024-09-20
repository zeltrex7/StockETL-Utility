import os
from tracemalloc import start 
import numpy as np
import pandas as pd # type: ignore
from datetime import date
from sqlalchemy import create_engine , text
from Scripts.mysql_helper_funtions import get_current_end_date, get_stock_mstr_key, insert_new_stock, update_stock_end_date ,create_db
import time
from dotenv import load_dotenv

load_dotenv(dotenv_path='./.env')

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
def main():
    import pysftp
    start_time = time.time()
    engine = create_engine("mysql+mysqlconnector://root:root@172.18.0.1:3307")
    # Define the SFTP server details
    hostname = '172.18.0.1'
    port = 2222
    username = 'abhishek'
    password = 'password'
    # Define connection options to disable host key checking
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # Disable host key checking
    # Establish an SFTP connection  
    local_path = '/opt/bitnami/consume_data/'
    if os.path.isdir(local_path):
        print("Directory exists.")
    else:
        print("Directory does not exist.")
        os.mkdir(local_path)
    #print(os.listdir(local_path))

    os.chdir(local_path)

    while(True):
        
        try:
            with pysftp.Connection(host=hostname, username=username, password=password, port=port, cnopts=cnopts) as sftp:
                sftp_remote_dir = '/config/sftp_stocks_data/'
                
                file_list = sftp.listdir(sftp_remote_dir)
                if len(file_list)  < 1 : 
                    break
                file_name = i = file_list[0]
                sftp.get(sftp_remote_dir+file_name)
                sftp.remove(sftp_remote_dir+file_name)
                sftp.close()
                #print(file_name)

            with engine.connect() as connection :     
                    print(f"File {file_name} Fetched successfully ")
                    stock_is_new = None
                    stock_name = i.replace(".csv",'')
                    temp =  connection.execute(text("SELECT * FROM stocks.stocks_master s WHERE s.stock_name = '"+stock_name+"';"))
                    data = pd.read_csv(file_name,delimiter=';',on_bad_lines="skip")
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
                    #print(stock_name,'-',stock_mstr_key)
                    
                    data = data[['Date','Open','High','Low','Close','Adj Close','Volume']]
                    data['stock_mstr_key'] = stock_mstr_key
                    data['Volume'] = data['Volume'].apply(convert_to_number)

                    if stock_is_new == 1:
                        data.to_sql('stocks_data', con=engine,schema='stocks', if_exists='append', index=False)
                    else:
                        end_date = get_current_end_date(stock_name)
                        data = data[data['Date'] > end_date]
                        if data.shape[0]==0:
                            print("No records to insert in Stock Data up-to date")
                            
                        else:
                            end_date = data['Date'].max()
                            update_stock_end_date(stock_name=stock_name, end_date=end_date) 
                            data.to_sql('stocks_data', con=engine,schema='stocks', if_exists='append', index=False)
                            print("Data Updated")
                    os.remove(i)
                    #print("file name : ",i)
            
            print("Data Load time taken : ",np.round(time.time()-start_time,2),' seconds')
        except :
            print('error')
        #os.rmdir(local_path)
                    
    
if __name__ == "__main__":
    main()
