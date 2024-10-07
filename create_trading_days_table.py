
import pandas as pd
import pymysql

path1 = r'C:\d\loop\stock_project\trading_day.csv'
data = pd.read_csv(path1)
# Define your database connection parameters

connection = pymysql.connect(
    host='localhost',
    user='root',
    password='root123',
    database='stock_data'
)

with connection.cursor() as cursor:
    for index, row in data.iterrows():
        sql = """
        INSERT INTO trading_days (Date, DateKey, Day, Month, MonthName, Quarter, QuarterName, Year, DayOfWeek, DayOfWeekName, IsWeekend, WeekOfYear, FirstDayOfMonth, LastDayOfMonth, EndOfWeek)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (
            row['Date'],
            row['DateKey'],
            row['Day'],
            row['Month'],
            row['MonthName'],
            row['Quarter'],
            row['QuarterName'],
            row['Year'],
            row['DayOfWeek'],
            row['DayOfWeekName'],
            row['IsWeekend'],
            row['WeekOfYear'],
            row['FirstDayOfMonth'],
            row['LastDayOfMonth'],
            row['EndOfWeek']
        ))
    connection.commit()

print("Data inserted into trading_days.")

connection.close()