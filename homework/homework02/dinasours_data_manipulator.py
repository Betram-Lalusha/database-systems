import pandas as pd
import sqlite3

df = pd.read_csv("dinasours_data.csv")
print(df.head())
print(df.tail())
print(df.columns)

connection = sqlite3.connect("dinasours.db")
cursor = connection.cursor()
cursor.execute("DROP TABLE IF EXISTS dinasour_data")

# create table
df.to_sql("dinasour_data", connection)

connection.close()