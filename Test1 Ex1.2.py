# Import relevant modules
import sqlite3
import pandas as pd

DB_FILE_PATH = "database.sqlite"

# Used Try catch blocks for exception handling,
# Inside Try block all queries are placed and errors are inside exception and
# close connection are inside finale block,
try:
    # Initialize  connection to our sqlite DB
    conn = sqlite3.connect(DB_FILE_PATH)
    print("SQLite Database connection successful")

    #
    # This query is used to get unique users with null post and reactions
    # Both tables are used LEFT JOIN to join user table with posts and reaction. AS is used to shorthand the code
    # WHERE is used to filter the result
    #
    query = """
    SELECT COUNT (DISTINCT u.id) AS inactive_users FROM users AS u
    LEFT JOIN posts AS p ON u.id = p.user_id
    LEFT JOIN reactions AS r ON u.id = r.user_id
    WHERE p.user_id IS NULL AND r.user_id IS NULL 
    """

    # Execute SQL Query
    result_df = pd.read_sql_query(query, conn)

    # Load it to the Pandas Data frame
    count = result_df['inactive_users'][0]

    print(f"Number of users with no posts or reactions: {count}")

except Exception as e:
    print(f"Exception found {e}")
finally:
    if conn:
        conn.close()
        print("Connection was closed X")