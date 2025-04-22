# === ETL pipeline for Country GDP Data ===
# This script extracts GDP data from a web page, transforms it, and loads it into both a CSV and a SQLite database.

from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

def extract(url, table_attribs):
    """
    Extracts country and GDP data from the given URL using BeautifulSoup.
    Returns a DataFrame with the specified table attributes.
    """
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')

    df = pd.DataFrame(columns=table_attribs)
    rows = soup.find_all('tbody')[2].find_all('tr')  # Target the correct table

    for row in rows:
        cols = row.find_all('td')
        if cols and cols[0].find('a') and '—' not in cols[2].text:
            country = cols[0].a.text
            gdp = cols[2].text
            df = pd.concat([df, pd.DataFrame({"Country": [country], "GDP_USD_millions": [gdp]})], ignore_index=True)

    return df

def transform(df):
    """
    Transforms the GDP values from millions to billions.
    Removes commas and rounds to two decimal places.
    Returns the transformed DataFrame.
    """
    df["GDP_USD_billions"] = [round(float(gdp.replace(",", "")) / 1000, 2) for gdp in df["GDP_USD_millions"]]
    return df[["Country", "GDP_USD_billions"]]  # Return only the required columns

def load_to_csv(df, path):
    """
    Saves the DataFrame to a CSV file at the specified path.
    """
    df.to_csv(path, index=False)

def load_to_db(df, conn, table):
    """
    Loads the DataFrame into a SQLite database as a table.
    Replaces the table if it already exists.
    """
    df.to_sql(table, conn, if_exists='replace', index=False)

def run_query(query, conn):
    """
    Executes a SQL query on the database and prints the result.
    """
    print(f"\nRunning Query:\n{query}")
    result = pd.read_sql(query, conn)
    print("\nQuery Output:\n", result)

def log_progress(message):
    """
    Logs the progress of the ETL steps with a timestamp to a log file.
    """
    now = datetime.now().strftime('%Y-%b-%d-%H:%M:%S')
    with open("etl_project_log.txt", "a") as f:
        f.write(f"{now} : {message}\n")

# === Define ETL Process Parameters ===
url = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
table_attribs = ["Country", "GDP_USD_millions"]
csv_path = "Countries_by_GDP.csv"
db_path = "World_Economies.db"
table_name = "Countries_by_GDP"

# === Execute ETL Steps ===
log_progress("ETL process started")

# Step 1: Extract
df = extract(url, table_attribs)
log_progress("Data extraction completed")

# Step 2: Transform
df = transform(df)
log_progress("Data transformation completed")

# Step 3a: Load to CSV
load_to_csv(df, csv_path)
log_progress("Data loaded into CSV")

# Step 3b: Load to SQLite DB
conn = sqlite3.connect(db_path)
log_progress("Database connection established")

load_to_db(df, conn, table_name)
log_progress("Data loaded into SQLite database")

# Step 4: Query
query = f"SELECT * FROM {table_name} WHERE GDP_USD_billions >= 100"
run_query(query, conn)
log_progress("Query executed")

conn.close()
log_progress("Database connection closed. ETL process completed")
