import sqlite3
import pandas as pd

# Step 1: Connect to (or create) the SQLite database
conn = sqlite3.connect('STAFF.db')

# Step 2: Define the table name and column names
table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

# Step 3: Read CSV data into a DataFrame
# Ensure the CSV file exists at the given path and has no header
file_path = 'INSTRUCTOR.csv'
df = pd.read_csv(file_path, names=attribute_list)

# Step 4: Create or replace the 'INSTRUCTOR' table and insert the data
df.to_sql(table_name, conn, if_exists='replace', index=False)
print('✅ Table created and data inserted.')

# Step 5: Run and display SQL queries

# Query 1: Select all rows
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(f"\n📄 Query: {query_statement}\n", query_output)

# Query 2: Select only first names
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(f"\n📄 Query: {query_statement}\n", query_output)

# Query 3: Count number of rows
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(f"\n📄 Query: {query_statement}\n", query_output)

# Step 6: Append a new instructor to the table
data_dict = {
    'ID': [100],
    'FNAME': ['John'],
    'LNAME': ['Doe'],
    'CITY': ['Paris'],
    'CCODE': ['FR']
}
data_append = pd.DataFrame(data_dict)

# Append the new data to the existing table
data_append.to_sql(table_name, conn, if_exists='append', index=False)
print('\n➕ New data appended successfully.')

# Step 7: Close the connection
conn.close()
