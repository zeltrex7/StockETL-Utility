import os
from dotenv import load_dotenv
import pymysql
from pymysql.err import OperationalError
connection = None
# Load environment variables from .env file
load_dotenv()

def create_db():
    connection = pymysql.connect(
        host=os.getenv("HOST"),
        port=int(os.getenv("PORT")),
        user=os.getenv("USERNAME"),
        password=os.getenv("PASSWORD")    )
    cursor = connection.cursor()
    cursor.execute("""CREATE database IF NOT EXISTS stocks;""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS stocks.stocks_master(
                      stock_id SMALLINT auto_increment primary key,
                      stock_name VARCHAR(50),
                      start_date DATE,
                      end_date DATE,
                      last_updated DATE
                      );""")
    print("Schema Created")
    connection.commit()
    

try:
    connection = pymysql.connect(
        host=os.getenv("HOST"),
        port=int(os.getenv("PORT")),
        database=os.getenv("DATABASE"),
        user=os.getenv("USERNAME"),
        password=os.getenv("PASSWORD"))
except OperationalError as e:
    # Error code 1049 is for "Unknown database"
    if e.args[0] == 1049:
        print(e)
        print("Database does not exist")
        create_db()
        exit(0)
    else:
        print(f"OperationalError: {e}")
        exit(1)
except Exception as e:
    print(f"An error occurred: {e}")
    exit(1)
else:
    cursor = connection.cursor()
    cursor.execute("SELECT @@version")
    version = cursor.fetchone()
    print(version[0])
finally:
    connection.close()