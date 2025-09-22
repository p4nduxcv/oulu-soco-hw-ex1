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
    # Here total_comments and total_reaction are consider as total engagements
    # calculations is simple total_engagements = total_comments + total_reaction
    # Finally get all total_engagement and sort them to descending order and limit it to 5
    #
    query = """
    SELECT
    u.username,
    COUNT(DISTINCT c.id) AS total_comments,
    COUNT(DISTINCT r.id) AS total_reactions,
    COUNT(DISTINCT c.id) + COUNT(DISTINCT r.id) AS total_engagement
    FROM users AS u
    JOIN posts AS p ON u.id = p.user_id
    LEFT JOIN comments AS c ON p.id = c.post_id
    LEFT JOIN reactions AS r ON p.id = r.post_id
    GROUP BY u.id
    ORDER BY total_engagement DESC
    LIMIT 5;
    """

    # Execute SQL Query
    top_users_df = pd.read_sql_query(query, conn)
    # Load it to the Pandas Data frame and select only username and total_enagagement
    top_users = top_users_df[['username', 'total_engagement']]


    print(f"5 Most engaged users with their total engagements: {top_users}")

except Exception as e:
    print(f"Exception found {e}")
finally:
    if conn:
        conn.close()
        print("Connection was closed X")