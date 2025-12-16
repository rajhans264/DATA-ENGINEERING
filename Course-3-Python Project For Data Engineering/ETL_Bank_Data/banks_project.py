# banks_project.py
# ETL Project: Top 10 Largest Banks by Market Cap

import os
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

# -------------------------------
# Initialize known values
# -------------------------------

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
csv_path = r"F:\DATA ENGINEERING\Course-3\ETL_Bank_Data\Largest_banks_data.csv"
folder = os.path.dirname(csv_path)
# Create DB path in the same folder
db_name = os.path.join(folder, "Bank_Project.db")
# db_name = "Banks.db"
table_name = "Largest_banks"
log_file = r"F:\DATA ENGINEERING\Course-3\ETL_Bank_Data\code_log.txt"
exchange_csv = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"

# -------------------------------
# Logging function
# -------------------------------
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + " : " + message + "\n")

# -------------------------------
# Extraction function
# -------------------------------
def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, "html.parser")

    df = pd.DataFrame(columns=table_attribs)
    # First "By market capitalization" table on the page
    table = data.find("table", {"class": "wikitable"})
    rows = table.find_all("tr")

    for row in rows[1:]:  # skip header
        col = row.find_all("td")
        if len(col) > 0:
            name = col[1].get_text(strip=True)
            mc_usd = col[2].get_text(strip=True)

            # clean Market Cap column
            mc_usd = mc_usd.replace(",", "").split()[0]
            try:
                mc_usd = float(mc_usd)
            except:
                continue

            df1 = pd.DataFrame({"Name": [name], "MC_USD_Billion": [mc_usd]})
            df = pd.concat([df, df1], ignore_index=True)

    return df.head(10)  # top 10 banks only
# -------------------------------
# Transformation function
# -------------------------------
def transform(df, exchange_csv):
    rates = pd.read_csv(exchange_csv, index_col=0).to_dict()["Rate"]

    df["MC_GBP_Billion"] = [np.round(x * rates["GBP"], 2) for x in df["MC_USD_Billion"]]
    df["MC_EUR_Billion"] = [np.round(x * rates["EUR"], 2) for x in df["MC_USD_Billion"]]
    df["MC_INR_Billion"] = [np.round(x * rates["INR"], 2) for x in df["MC_USD_Billion"]]

    return df

# -------------------------------
# Load functions
# -------------------------------
def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)

# -------------------------------
# Query function
# -------------------------------
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# -------------------------------
# Code execution
# -------------------------------
log_progress("Preliminaries complete. Initiating ETL process")

df = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")

df = transform(df, exchange_csv)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df, csv_path)
log_progress("Data saved to CSV file")

sql_connection = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as table. Running the query")

# Example queries
run_query(f"SELECT * FROM {table_name}", sql_connection)
run_query(f"SELECT AVG(MC_GBP_Billion) FROM {table_name}", sql_connection)
run_query(f"SELECT Name FROM {table_name} LIMIT 5", sql_connection)

log_progress("Process Complete")
sql_connection.close()
log_progress("Server Connection closed")
