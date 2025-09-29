# SQLite and data analysis libraries
import sqlite3
import pandas
import numpy
import matplotlib.pyplot as plt

# Optional for clearing the terminal
import os
os.system('clear')

DB_FILE = "database.sqlite"
try:
    conn = sqlite3.connect(DB_FILE)

    # Use pandas to load SQLite data into dataframes
    users = pandas.read_sql_query("SELECT * FROM users", conn)
    posts = pandas.read_sql_query("SELECT * FROM posts", conn)
    comments = pandas.read_sql_query("SELECT * FROM comments", conn)
    reactions = pandas.read_sql_query("SELECT * FROM reactions", conn)

except Exception as e:
    print(f"Uh oh '{e}'")
finally:
    if conn:
        conn.close()
        print("SQLite Database connection closed.\n")

import sqlite3
import pandas

#Load data from the database into DataFrames
try:
    conn = sqlite3.connect("database.sqlite")
    posts = pandas.read_sql_query("SELECT * FROM posts", conn)
    comments = pandas.read_sql_query("SELECT * FROM comments", conn)
    reactions = pandas.read_sql_query("SELECT * FROM reactions", conn)
    users = pandas.read_sql_query("SELECT * FROM users", conn)
except Exception as e:
    print(f"Error: {e}")
finally:
    if conn:
        conn.close()

# Count Reactions and Comments for each post Group by post_id and count the number of reactions for each
reaction_counts = reactions.groupby('post_id').size().reset_index(name='reaction_count')

# Group by post_id and count the number of comments for each
comment_counts = comments.groupby('post_id').size().reset_index(name='comment_count')

# Merge data into a single DataFrame begin with the posts table
merged_data = posts

# Merge the reaction counts data frame. Use a 'left' merge to keep posts with 0 reactions.
merged_data = pandas.merge(merged_data, reaction_counts, left_on='id', right_on='post_id', how='left')

# Merge the comment counts data frame. Also a 'left' merge to keep posts with 0 comments.
merged_data = pandas.merge(merged_data, comment_counts, left_on='id', right_on='post_id', how='left')

# Assumption - Replace NaN (for posts with no reactions/comments) with 0, id there are Nan values, calculation might be affected
merged_data['reaction_count'] = merged_data['reaction_count'].fillna(0)
merged_data['comment_count'] = merged_data['comment_count'].fillna(0)

# Assumption - Calculate the score using our 3:1 weighted formula beause comments has more weight than reaction
# reaction can be done single click, but commiting has few steps which is think, write, button clicks so considering
# this following calculation is formated.
merged_data['virality_score'] = (merged_data['comment_count'] * 3) + (merged_data['reaction_count'] * 1)


# Join with users table to get the author's username to better understanding.
final_data = pandas.merge(merged_data, users[['id', 'username']], left_on='user_id', right_on='id', how='left')

# Sort the entire dataset by the new score in descending order to get more viral content 1st and select 1st 3 head(3)
top_posts = final_data.sort_values(by='virality_score', ascending=False).head(3)

# Display the final result with relevant columns
print("3 Most Viral Post ")
print(top_3_viral[['post_id_x', 'username', 'content', 'comment_count', 'reaction_count', 'virality_score']])

conn.close()