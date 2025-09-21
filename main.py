# Import relevant modules
import sqlite3
import pandas as pd

# Define path to our DB
DB_FILE_PATH = "database.sqlite"

# Used Try catch blocks for exception handling, Inside Try block,
try:
    # Initialize  connection to our sqlite DB
    conn = sqlite3.connect(DB_FILE_PATH)
    print("SQLite Database connection successful")

    # Query to get all table names
    query = "SELECT name FROM sqlite_master WHERE type='table'"
    # Execute SQL Query and load it to the Pandas Data frame
    tablenames_df = pd.read_sql_query(query, conn)

    # Push these table for python List for further usage
    table_names = tablenames_df['name'].tolist()
    print(f"Available Tables: {table_names}")

    # Loop is used to get num. of row and columns. This is the purpose of add them to the List.
    for table_name in table_names:
        print(f"Table - {table_name}")
        row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        rows_df = pd.read_sql_query(row_count_query, conn)
        #
        # Here we are getting index ( for a example <index> <count>)  as well prevent this,
        # iloc (index location) is used, then we can select first columns without index.
        #
        num_rows = rows_df.iloc[0, 0]
        print(f"Number of rows: {num_rows}")

        #
        # Get the columns and their count for the current table, PRAGMA is SQLite specific command,
        # it is a smart way to use return values that describe the columns of the relevant table
        columns_query = f"PRAGMA table_info({table_name})"
        columns_df = pd.read_sql_query(columns_query, conn)
        column_names = columns_df['name'].tolist()

        print(f"Columns: {', '.join(column_names)}\n")

except Exception as e:
    print(f"Exception found {e}")




