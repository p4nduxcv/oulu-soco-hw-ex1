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
    # Here UNION ALL is used to combined two subqueries (post and comments)
    # grouping user_id and content hereafter HAVING is used to filter groups  with count >= 3
    # Finally Join the combined spam table with user table to get respective user details
    # spam.content and spam.repetition_count is coming from subquery alias Spam that we created, This-
    # subquery temp table with user_id, content, repetition count
    #
    query = """
    SELECT
    u.username,
    spam.content,
    spam.repetition_count
    FROM users AS u
    JOIN
    (
    SELECT user_id, content, COUNT(*) AS repetition_count FROM posts GROUP BY user_id, content HAVING COUNT(*) >= 3
    UNION ALL
    SELECT user_id, content, COUNT(*) AS repetition_count FROM comments GROUP BY user_id, content HAVING COUNT(*) >= 3
    ) 
    AS spam ON u.id = spam.user_id;
    """

    # Execute SQL Query
    spammers_df = pd.read_sql_query(query, conn)
    # # Load it to the Pandas Data frame and select only username
    spammers = spammers_df['username']

    print(f"Number of Scammers: {spammers}")

except Exception as e:
    print(f"Exception found {e}")
finally:
    if conn:
        conn.close()
        print("Connection was closed X")