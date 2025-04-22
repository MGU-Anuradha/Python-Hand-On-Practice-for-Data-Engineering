from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

# ======================== EXTRACT STEP ========================
def extract(url, table_attribs):
    """
    Extracts the largest banks and their market capitalization in USD from the given URL.
    """
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')

    df = pd.DataFrame(columns=table_attribs)

    # Locate the table body
    tables = soup.find_all('tbody')
    if len(tables) < 1:
        print("No tables found on the page.")
        return df

    rows = tables[0].find_all('tr')

    for row in rows:
        cols = row.find_all('td')
        if len(cols) >= 3:
            name = cols[1].text.strip()
            usd = cols[2].text.strip().replace(",", "")
            df = pd.concat([df, pd.DataFrame({"Name": [name], "MC_USD_Billion": [usd]})], ignore_index=True)

    return df

# ======================== TRANSFORM STEP ========================
def transform(df):
    """
    Transforms the USD Market Cap to GBP, EUR, and INR using exchange rates provided in a CSV.
    """
    exchange_df = pd.read_csv("exchange_rate.csv")
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']

    df["MC_USD_Billion"] = df["MC_USD_Billion"].astype(float)

    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    return df

# ======================== LOAD STEP ========================
def load_to_csv(df, path):
    """
    Saves the DataFrame to a CSV file at the specified path.
    """
    df.to_csv(path, index=False)

def load_to_db(df, conn, table):
    """
    Loads the DataFrame into a SQLite database table. Replaces the table if it exists.
    """
    df.to_sql(table, conn, if_exists='replace', index=False)

# ======================== QUERY STEP ========================
def run_queries(query, conn):
    """
    Executes a SQL query on the database and prints the result.
    """
    print(f"\nRunning Query:\n{query}")
    result = pd.read_sql(query, conn)
    print("\nQuery Output:\n", result)

# ======================== LOGGING STEP ========================
def log_progress(message):
    """
    Logs the progress of the ETL steps with a timestamp to a log file.
    """
    now = datetime.now().strftime('%Y-%b-%d-%H:%M:%S')
    with open("code_log.txt", "a") as f:
        f.write(f"{now} : {message}\n")

# ======================== ETL EXECUTION ========================
if __name__ == "__main__":
    # Define ETL Process Parameters
    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    table_attribs = ["Name", "MC_USD_Billion"]
    csv_path = "Largest_banks_data.csv"
    db_path = "Banks.db"
    table_name = "Largest_banks"

    # Step 1: Extract
    log_progress("Starting data extraction...")
    df = extract(url, table_attribs)
    print("\nExtracted Data:")
    print(df)
    log_progress("Data extraction completed")

    # Step 2: Transform
    df = transform(df)
    print("\nTransformed Data:")
    print(df)
    print("Market Capitalization of 5th largest bank in EUR (Billion):", df['MC_EUR_Billion'][4])
    log_progress("Data transformation completed")

    # Step 3a: Load to CSV
    load_to_csv(df, csv_path)
    log_progress("Data loaded into CSV")

    # Step 3b: Load to SQLite DB
    conn = sqlite3.connect(db_path)
    log_progress("Database connection established")
    load_to_db(df, conn, table_name)
    log_progress("Data loaded into SQLite database")

    # Step 4: Queries
    run_queries("SELECT * FROM Largest_banks", conn)
    run_queries("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", conn)
    run_queries("SELECT Name FROM Largest_banks LIMIT 5", conn)
    log_progress("Query executed")

    conn.close()
    log_progress("Database connection closed. ETL process completed")
