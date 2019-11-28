import mysql.connector
import sqlalchemy
import pandas as pd


data = [['tom', 10], ['nick', 15], ['juli', 14]] 
  
# Create the pandas DataFrame 
df = pd.DataFrame(data, columns = ['Name', 'Age']) 
database_username = 'root'
database_password = 'www777#A'
database_ip       = '127.0.0.1'
database_name     = 'all_stocks'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name), pool_recycle=1, pool_timeout=57600).connect()

df.to_sql(con=database_connection, name='university_dataset_ca', if_exists='replace',chunksize=100)
database_connection.close()