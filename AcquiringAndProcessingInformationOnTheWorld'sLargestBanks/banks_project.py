import sqlite3
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
tableAttributes = ['Name', 'MC_USD_Billion']
tableAttributes2 = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
dbName = 'Banks.db'
tableName = 'Largest_banks'
logFile = 'code_log.txt'
target_file = "./Largest_banks_data.csv"

def extract(url, tableattributes):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, "html.parser")
    df = pd.DataFrame(columns = tableattributes)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        cols = row.find_all('td')

        if len(cols) != 0:
            direct_a_tags = [child for child in cols[1].children if child.name == 'a']
            data_dict = {'Name': direct_a_tags[0].contents[0],
                        'MC_USD_Billion': cols[2].contents[0]}
            df1 = pd.DataFrame(data_dict, index = [0])
            df = pd.concat([df, df1], ignore_index = True)
    return df

def transform(df):
    dataframe = pd.read_csv('exchange_rate.csv')
    exchange_rate = dataframe.set_index('Currency').to_dict()['Rate']
    df['MC_USD_Billion'] = [float(x[:-2]) for x in df['MC_USD_Billion']]
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    timestamp_format = '%Y:%h:%d:%H:%M:%S'
    time = datetime.now()
    timestamp = time.strftime(timestamp_format)
    with open(logFile, 'a') as file:
        file.write(timestamp + ' : ' + message + '\n')


log_progress('Preliminaries completed. Initiating ETL process')

df = extract(url,tableAttributes)

log_progress('Data extraction complete. Initiating transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, target_file)

log_progress('Data saved to csv file')

sql_connection = sqlite3.connect(dbName)

log_progress('SQL connection initiated')

load_to_db(df, sql_connection, tableName)

log_progress('Data loaded to Database as table. Running the query')
query_statement_1 = "SELECT * FROM Largest_banks"
run_query(query_statement_1, sql_connection)

query_statement_2 = "Select AVG(MC_GBP_Billion) from Largest_banks"
run_query(query_statement_2, sql_connection)

query_statement_3 = "Select Name, MC_EUR_Billion from Largest_banks Limit 5"
run_query(query_statement_3, sql_connection)

log_progress('Process complete.')

sql_connection.close()

