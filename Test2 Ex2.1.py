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

# Getting all platform growth by year and Ensure created_at is a datetime object and extract the year for all tables
users['year'] = pandas.to_datetime(users['created_at']).dt.year
posts['year'] = pandas.to_datetime(posts['created_at']).dt.year
comments['year'] = pandas.to_datetime(comments['created_at']).dt.year

# Reactions don't have a timestamp, so merge with posts to get one
reactions_with_date = pandas.merge(
    reactions, posts[['id', 'created_at']], left_on='post_id', right_on='id'
)
reactions_with_date['year'] = pandas.to_datetime(reactions_with_date['created_at']).dt.year

# Here, we dont have date-related data in the reactions table, hence we merge with the post to obtain it.
user_counts = users.groupby('year').size()
post_counts = posts.groupby('year').size()
comment_counts = comments.groupby('year').size()
reaction_counts = reactions_with_date.groupby('year').size()

# Combine all event data to create another data frame called year-events_count
yearly_events_count = pandas.DataFrame({
    'users': user_counts,
    'posts': post_counts,
    'comments': comment_counts,
    'reactions': reaction_counts
}).fillna(0)

# Sum all events for a total yearly figure
yearly_events_count['total_events'] = yearly_events_count.sum(axis=1)
yearly_events_count = yearly_events_count.reset_index() # Convert year from index to column

print("Yearly Platform Growth (Users + Activities)")
print(yearly_events_count[['year', 'total_events']])

# Identifying the  growth estimate by removing outliers (1.5 IQR standard)

#Calculate the raw year-over-year growth in total event values. dropna() is used to remove Nan values, because in the very first column we dont have a value to reduce
yearly_events_count['yearly_growth'] = yearly_events_count['total_events'].diff()
growth_data = yearly_events_count[['year', 'yearly_growth']].dropna()


# Calculate Q1, Q3, and IQR for the yearly growth figures
q1 = growth_data['yearly_growth'].quantile(0.25)
q3 = growth_data['yearly_growth'].quantile(0.75)
iqr = q3 - q1

# Define bounds for non-outliers
lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr

# Filter growth data to exclude outliers
filtered_growth_usage = growth_data[
    (growth_data['yearly_growth'] >= lower_bound) &
    (growth_data['yearly_growth'] <= upper_bound)
]

#Predict and Calculate Server Needs ---

# Calculate the average growth usage from the stable (non-outlier) years
avg_stable_growth = filtered_growth_usage['yearly_growth'].mean()

print(f"avg---->{avg_stable_growth}")

# Get the latest event data from the above DF. -1 is used to get the last item
events_now = yearly_events_count['total_events'].iloc[-1]
last_year = yearly_events_count['year'].iloc[-1]

# Predict the load 3 years from now (2026, 2027, 2028)
forecasted_events = events_now + (avg_stable_growth * 3)
target_year = last_year + 3 #until 2028

# Calculate server needs based on this comprehensive forecast and numpy is used to round up the final values
current_servers = 16
events_per_server = events_now / current_servers
servers_for_growth = forecasted_events / events_per_server
servers_with_redundancy = servers_for_growth + (servers_for_growth*0.2)
final_server_count = numpy.ceil(servers_with_redundancy)

print(f"\n--- Server Requirement Predictions ---")
print(f"Last data year: {last_year}")
print(f"Current total events: {events_now:,.0f}")
print(f"Stable average yearly growth (outliers removed): {avg_stable_growth:,.0f} events/year")
print(f"Predicted events in {target_year}: {forecasted_events:,.0f}")
print(f"Servers needed with 20% redundancy: {servers_with_redundancy:.2f}")
print(f"Final server count (rounded up): {int(final_server_count)}")

#Plot the results
plt.style.use('seaborn-v0_8-whitegrid')
fig, ax = plt.subplots(figsize=(10, 6))

# Plot historical data
ax.plot(yearly_events_count['year'], yearly_events_count['total_events'], 'o-', color='blue', label='Growth')

# Create data for a continuous forecast line
forecast_plot_years = [last_year] + list(numpy.arange(last_year + 1, target_year + 1))
forecast_plot_events = [events_now] + [events_now + avg_stable_growth * i for i in range(1, 4)]

# Plot the continuous line
ax.plot(forecast_plot_years, forecast_plot_events, 'o--', color='red', label='Predicted Growth')

ax.set_title('Platform Growth Forecast (IQR-Refined)', fontsize=16)
ax.set_xlabel('Year')
ax.set_ylabel('Total Growth (Users + Interactions)')
ax.legend()
plt.xticks(numpy.arange(yearly_events_count['year'].min(), target_year + 1, 1))
ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))

plt.show()

conn.close()