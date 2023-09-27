import mechanicalsoup
import sqlite3
import pandas as pd

url = "https://en.wikipedia.org/wiki/2022–23_Premier_League"
browser = mechanicalsoup.StatefulBrowser()
browser.open(url)

td = browser.page.find_all("td")
rows_data = [value.text.replace("\n", "") for value in td]
rows_data = rows_data[23:103]

th = rows = browser.page.find_all("th")
columns_data = [value.text.replace("\n", "") for value in th]
columns_data = columns_data[22:26]

dictionary = {}

def get_column_info(start_index):
    data_list = []
    repeat = start_index
    while (repeat < len(rows_data)):
        data_list.append(rows_data[repeat])
        repeat += 4
    return data_list

for index, column_name in enumerate(columns_data):
    data_list = get_column_info(index)
    dictionary[column_name] = data_list

df = pd.DataFrame(data=dictionary)

connection = sqlite3.connect("premier_leage_table.db")
cursor = connection.cursor()
cursor.execute("create table if not exists premier_league (" + ",".join(columns_data) +")")
for i in range(len(df)):
    cursor.execute("insert into premier_league values (?,?,?,?)", df.iloc[i])

connection.commit()

connection.close()