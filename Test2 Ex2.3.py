import sqlite3
import pandas as pd


try:
    #Load data from the database into DataFrames
    conn = sqlite3.connect("database.sqlite")
    posts = pd.read_sql_query("SELECT id, created_at FROM posts", conn)
    comments = pd.read_sql_query("SELECT post_id, created_at FROM comments", conn)
except Exception as e:
    print(f"Uh oh '{e}'")
finally:
    if conn:
        conn.close()

# Prepare data by converting to datetime objects because this step is crucial for performing date/time calculations convert the text based timestamps to specialized datetime object which is lead python to do the calculation
posts['created_at'] = pd.to_datetime(posts['created_at'])
comments['created_at'] = pd.to_datetime(comments['created_at'])

# Find the first and last commenting time for each post
# We used groupby 'post_id' and find the minimum and maximum comment timestamps using .agg() this allows to identify min and mx values properly without lengthy calculations
comment_times = comments.groupby('post_id')['created_at'].agg(['min', 'max']).reset_index()

# Merge post creation times with engaged times
merged_data = pd.merge(posts, comment_times, left_on='id', right_on='post_id')

# Calculate the time difference between for each post and Subtracting datetime columns
merged_data['time_to_first'] = merged_data['min'] - merged_data['created_at']
merged_data['time_to_last'] = merged_data['max'] - merged_data['created_at']

# Calculate and mean() is used to get average value
avg_time_to_first_engagement = merged_data['time_to_first'].mean()
avg_time_to_last_engagement = merged_data['time_to_last'].mean()

print("Lifecycle Averages as Follows")
print(f"Average time to first engagement: {avg_time_to_first_engagement}")
print(f"Average time to last engagement: {avg_time_to_last_engagement}")