
import pandas as pd
import pymysql


# Define your database connection parameters
output_path = r'C:\d\loop\stock_project\final_trading_data.csv'
trading_data = pd.read_csv(output_path)

connection = pymysql.connect(
    host='localhost',
    user='root',
    password='root123',
    database='stock_data'
)

with connection.cursor() as cursor:
    for index, row in trading_data.iterrows():
        sql = """
        INSERT INTO stock_daily_staging_table (Date, Symbol, Open, High, Low, Close, Adj_Close, Volume, Turnover)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (
            row['Date'],
            row['Symbol'],
            row['Open'],
            row['High'],
            row['Low'],
            row['Close'],
            row['Adj Close'],
            row['Volume'],
            row['Turnover']
        ))
    connection.commit()

print("Data inserted into stock_daily_staging_table.")

connection.close()
