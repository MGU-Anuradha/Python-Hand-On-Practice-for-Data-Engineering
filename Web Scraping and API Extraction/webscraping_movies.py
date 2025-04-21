import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

# URL of the archived web page containing the list of highly ranked films
url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'

# Database and file paths
db_name = 'Movies.db'  # SQLite database name
table_name = 'Top_50'  # Table name in the database
csv_path = '/home/project/top_50_films.csv'  # Path to save the CSV file

# Create an empty DataFrame with specific columns
df = pd.DataFrame(columns=["Average Rank", "Film", "Year"])

# Fetch HTML content of the page
response = requests.get(url)
html_page = response.text

# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(html_page, 'html.parser')

# Find all <tbody> tags (which contain table rows)
tables = soup.find_all('tbody')

# Extract all rows from the first <tbody>
rows = tables[0].find_all('tr')

# Counter to limit to top 50 movies
count = 0

# Loop through each row to extract movie data
for row in rows:
    if count < 50:
        # Get all <td> columns in the current row
        cols = row.find_all('td')

        # Ensure the row contains columns
        if len(cols) >= 3:
            # Extract text and clean it
            average_rank = cols[0].get_text(strip=True)
            film_name = cols[1].get_text(strip=True)
            year = cols[2].get_text(strip=True)

            # Append the extracted data to the DataFrame
            df.loc[len(df)] = [average_rank, film_name, year]
            count += 1
    else:
        break  # Stop after top 50

# Print the final DataFrame
print(df)

# Save the DataFrame to a CSV file
df.to_csv(csv_path, index=False)
print(f"CSV saved to {csv_path}")

# Connect to SQLite database (it will be created if it doesn't exist)
conn = sqlite3.connect(db_name)

# Save the DataFrame to the SQLite database as a table
df.to_sql(table_name, conn, if_exists='replace', index=False)
print(f"Data written to SQLite database '{db_name}' in table '{table_name}'")

# Close the database connection
conn.close()
