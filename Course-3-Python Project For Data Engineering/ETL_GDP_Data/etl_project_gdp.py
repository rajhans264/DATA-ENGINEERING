"""
Project Scenario:
An international firm that is looking to expand its business in different countries across the world has recruited you. You have been hired as a junior Data Engineer and are tasked with creating an automated script that can extract the list of all countries in order of their GDPs in billion USDs (rounded to 2 decimal places), 
as logged by the International Monetary Fund (IMF). Since IMF releases this evaluation twice a year, this code will be used by the organization to extract the information as it is updated.
The required data seems to be available on the URL mentioned below:
URL:'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
The required information needs to be made accessible as a CSV file Countries_by_GDP.csv as well as a table Countries_by_GDP in a database file World_Economies.db with attributes Country and GDP_USD_billion.
Your boss wants you to demonstrate the success of this code by running a query on the database table to display only the entries with more than a 100 billion USD economy. Also, you should log in a file with the entire process of execution named etl_project_log.txt.
You must create a Python code 'etl_project_gdp.py' that performs all the required tasks.
"""
"""
Objectives
You have to complete the following tasks for this project
Write a data extraction function to retrieve the relevant information from the required URL.
Transform the available GDP information into 'Billion USD' from 'Million USD'.
Load the transformed information to the required CSV file and as a database file.
Run the required query on the database.
Log the progress of the code with appropriate timestamps.
"""
"""
Initial setup
Before you start building the code, you need to install the required libraries for it.

The libraries needed for the code are as follows:
requests - The library used for accessing the information from the URL.
bs4 - The library containing the BeautifulSoup function used for webscraping.
pandas - The library used for processing the extracted data, storing it to required formats and communicating with the databases.
sqlite3 - The library required to create a database server connection.
numpy - The library required for the mathematical rounding operation as required in the objectives.
datetime - The library containing the function datetime used for extracting the timestamp for logging purposes.
As discussed before, use the following command format in a terminal window to install the libraries.
"""
# As per the requirement, write the commands in etl_project_gdp.py at the position specified in the code structure, to import the relevant libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

"""
Further, you need to initialize all the known entities. These are mentioned below:

URL:'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs: The attributes or column names for the dataframe stored as a list. Since the data available in the website is in USD Millions, the attributes should initially be 'Country' and 'GDP_USD_millions'. This will be modified in the transform function later.
db_name: As mentioned in the Project scenario, 'World_Economies.db'
table_name: As mentioned in the Project scenario, 'Countries_by_GDP'
csv_path: As mentioned in the Project scenario, 'Countries_by_GDP.csv'
You should log the initialization process
"""
# Directry : F:\DATA ENGINEERING\Course-3\ETL_GDP_Data

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = 'World_Economies.db'
# db_name = r'F:\DATA ENGINEERING\Course-3\ETL_GDP_Data\World_Economies.db'
table_name ='Countries_by_GDP'
csv_path = r'F:\DATA ENGINEERING\Course-3\ETL_GDP_Data\Countries_by_GDP.csv'

"""
Task 1: Extracting information:
Extraction of information from a web page is done using the web scraping process. For this, you'll have to analyze the link and come up with the strategy of how to get the required information. The following points are worth observing for this task.
Inspect the URL and note the position of the table. Note that even the images with captions in them are stored in tabular format. Hence, in the given webpage, our table is at the third position, or index 2. Among this, we require the entries under 'Country/Territory' and 'IMF -> Estimate'.
Note that there are a few entries in which the IMF estimate is shown to be '—'. Also, there is an entry at the top named 'World', which we do not require. Segregate this entry from the others because this entry does not have a hyperlink and all others in the table do. So you can take advantage of that and access only the rows for which the entry under 'Country/Terriroty' has a hyperlink associated with it.
Note that '—' is a special character and not a general hyphen, '-'. Copy the character from the instructions here to use in the code.
Assuming the function gets the URL and the table_attribs parameters as arguments, complete the function extract() in the code following the steps below.
"""
def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page ,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country":col[0].a.contents[0], "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df

"""
Task 2: Transform information
The transform function needs to modify the ‘GDP_USD_millions’. 
You need to cover the following points as a part of the transformation process.
Convert the contents of the 'GDP_USD_millions' column of df dataframe from currency format to floating numbers.
"""
def transform(df):
    GDP_list = df['GDP_USD_millions'].tolist()
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
    return df

"""
Task 3: Loading information
Loading process for this project is two fold.
You have to save the transformed dataframe to a CSV file. 
For this, pass the dataframe df and the CSV file path to the function load_to_csv() and add the required statements there.
"""
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)
"""
You have to save the transformed dataframe as a table in the database. 
This needs to be implemented in the function load_to_db(), which accepts the dataframe df, 
the connection object to the SQL database conn, and the table name variable table_name to be used.
"""
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

"""
Task 4: Querying the database table
Assuming that the appropriate query was initiated and the query statement has been passed to the function run_query(), along with the SQL connection object sql_connection and the table name variable table_name, 
this function should run the query statement on the table and retrieve the output as a filtered dataframe. This dataframe can then be simply printed.
"""
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

"""
Task 5: Logging progress
Logging needs to be done using the log_progress() funciton. 
This function will be called multiple times throughout the execution of this code and will be asked to add a log entry in a .txt file, etl_project_log.txt. 
The entry is supposed to be in the following format:
"""
def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(r"F:\DATA ENGINEERING\Course-3\ETL_GDP_Data\etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')


# code execution :

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df)
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('World_Economies.db')
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)
log_progress('Process Complete.')
sql_connection.close()

# -------------------------------------------------------------------------------------------------------------------------------
# Code for the lab Execution
# banks_project.py
# ETL Project: Top 10 Largest Banks by Market Cap

# # import os
# from bs4 import BeautifulSoup
# import requests
# import pandas as pd
# import numpy as np
# import sqlite3
# from datetime import datetime
# # python3.11 -m pip install pandas
# # python3.11 -m pip install numpy
# # python3.11 -m pip install sqlite3

# # -------------------------------
# # Initialize known values
# # -------------------------------

# url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
# table_attribs = ["Name", "MC_USD_Billion"]
# csv_path = "./Largest_banks_data.csv"
# # folder = os.path.dirname(csv_path)
# # Create DB path in the same folder
# # db_name = os.path.join(folder, "Bank_Project.db")
# db_name = "Banks.db"
# table_name = "Largest_banks"
# log_file = "code_log.txt"
# exchange_csv = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"

# # -------------------------------
# # Logging function
# # -------------------------------
# def log_progress(message):
#     timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
#     now = datetime.now()
#     timestamp = now.strftime(timestamp_format)
#     with open(log_file, "a") as f:
#         f.write(timestamp + " : " + message + "\n")

# # -------------------------------
# # Extraction function
# # -------------------------------
# def extract(url, table_attribs):
#     page = requests.get(url).text
#     data = BeautifulSoup(page, "html.parser")

#     df = pd.DataFrame(columns=table_attribs)
#     # First "By market capitalization" table on the page
#     table = data.find("table", {"class": "wikitable"})
#     rows = table.find_all("tr")

#     for row in rows[1:]:  # skip header
#         col = row.find_all("td")
#         if len(col) > 0:
#             name = col[1].get_text(strip=True)
#             mc_usd = col[2].get_text(strip=True)

#             # clean Market Cap column
#             mc_usd = mc_usd.replace(",", "").split()[0]
#             try:
#                 mc_usd = float(mc_usd)
#             except:
#                 continue

#             df1 = pd.DataFrame({"Name": [name], "MC_USD_Billion": [mc_usd]})
#             df = pd.concat([df, df1], ignore_index=True)

#     return df.head(10)  # top 10 banks only
# # -------------------------------
# # Transformation function
# # -------------------------------
# def transform(df, exchange_csv):
#     rates = pd.read_csv(exchange_csv, index_col=0).to_dict()["Rate"]

#     df["MC_GBP_Billion"] = [np.round(x * rates["GBP"], 2) for x in df["MC_USD_Billion"]]
#     df["MC_EUR_Billion"] = [np.round(x * rates["EUR"], 2) for x in df["MC_USD_Billion"]]
#     df["MC_INR_Billion"] = [np.round(x * rates["INR"], 2) for x in df["MC_USD_Billion"]]

#     return df

# # -------------------------------
# # Load functions
# # -------------------------------
# def load_to_csv(df, csv_path):
#     df.to_csv(csv_path, index=False)

# def load_to_db(df, sql_connection, table_name):
#     df.to_sql(table_name, sql_connection, if_exists="replace", index=False)

# # -------------------------------
# # Query function
# # -------------------------------
# def run_query(query_statement, sql_connection):
#     print(query_statement)
#     query_output = pd.read_sql(query_statement, sql_connection)
#     print(query_output)

# # -------------------------------
# # Code execution
# # -------------------------------
# log_progress("Preliminaries complete. Initiating ETL process")

# df = extract(url, table_attribs)
# log_progress("Data extraction complete. Initiating Transformation process")

# df = transform(df, exchange_csv)
# log_progress("Data transformation complete. Initiating Loading process")

# load_to_csv(df, csv_path)
# log_progress("Data saved to CSV file")

# sql_connection = sqlite3.connect(db_name)
# log_progress("SQL Connection initiated")

# load_to_db(df, sql_connection, table_name)
# log_progress("Data loaded to Database as table. Running the query")

# # Example queries
# run_query(f"SELECT * FROM {table_name}", sql_connection)
# run_query(f"SELECT AVG(MC_GBP_Billion) FROM {table_name}", sql_connection)
# run_query(f"SELECT Name FROM {table_name} LIMIT 5", sql_connection)

# log_progress("Process Complete")
# sql_connection.close()
# log_progress("Server Connection closed")
