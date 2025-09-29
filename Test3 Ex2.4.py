# SQLite and data analysis libraries
import sqlite3
import pandas as pd

# Optional for clearing the terminal
import os
os.system('clear')

DB_FILE = "database.sqlite"
try:
    conn = sqlite3.connect(DB_FILE)

    # Use pandas to load SQLite data into dataframes
    posts = pd.read_sql_query("SELECT id, user_id FROM posts", conn)
    comments = pd.read_sql_query("SELECT user_id, post_id FROM comments", conn)
    reactions = pd.read_sql_query("SELECT user_id, post_id FROM reactions", conn)
    users = pd.read_sql_query("SELECT id, username FROM users", conn)

except Exception as e:
    print(f"Uh oh '{e}'")
finally:
    if conn:
        conn.close()
        print("SQLite Database connection closed.\n")

# Combine all comments and reactions into one. We take the user_id and post_id from both comments and reactions tables
all_engagements = pd.concat([
    comments[['user_id', 'post_id']],
    reactions[['user_id', 'post_id']]
])
# rename columns to better understanding
all_engagements.rename(columns={'user_id': 'interactor_id'}, inplace=True)

# Link each interaction to the user who owns the post
# We are look up the post owner for every engagement
# The result is a list of (who_engaged, who_owns_post) pairs
engagements_with_owner = pd.merge(all_engagements, posts, left_on='post_id', right_on='id')
# rename columns to better understanding
engagements_with_owner.rename(columns={'user_id': 'owner_id'}, inplace=True)

# Count interactions for each directed pair (A -> B). This provides us to the one-way engagement counts
directed_counts = engagements_with_owner.groupby(['interactor_id', 'owner_id']).size().reset_index(name='interaction_count')

# Create a "canonical pair" to sum of mutual engagements
# To treat (A, B) and (B, A) as the same, we put the same ID first
# lambda is anonymous function to perform simple task at this moment it called row
directed_counts['userA'] = directed_counts.apply(lambda row: min(row['interactor_id'], row['owner_id']), axis=1)
directed_counts['userB'] = directed_counts.apply(lambda row: max(row['interactor_id'], row['owner_id']), axis=1)

# Calculate the final mutual score for each pairs
# Now we can group by the canonical pair and sum the interactions
mutual_scores = directed_counts.groupby(['userA', 'userB'])['interaction_count'].sum().reset_index()

# Getting top 3 pairs Sort by interaction_count and find the top 3 pairs
top_3_pairs = mutual_scores.sort_values(by='interaction_count', ascending=False).head(3)

# Look up username for userA
top_3_with_names = pd.merge(top_3_pairs, users, left_on='userA', right_on='id')
top_3_with_names.rename(columns={'username': 'usernameA'}, inplace=True)
# Look up username for userB
top_3_with_names = pd.merge(top_3_with_names, users, left_on='userB', right_on='id')
top_3_with_names.rename(columns={'username': 'usernameB'}, inplace=True)

print(top_3_with_names[['usernameA', 'usernameB', 'interaction_count']])

conn.close()