import pandas as pd
import pymysql
df = pd.read_csv(r'C:\d\loop\stock_project\final_trading_data.csv', parse_dates=['Date'])

# Sort the DataFrame by Symbol and Date
df.sort_values(by=['Symbol', 'Date'], inplace=True)

# Calculate moving averages
df['moving_avg_7'] = df.groupby('Symbol')['Close'].transform(lambda x: x.rolling(window=7, min_periods=1).mean())
df['moving_avg_14'] = df.groupby('Symbol')['Close'].transform(lambda x: x.rolling(window=14, min_periods=1).mean())
df['moving_avg_21'] = df.groupby('Symbol')['Close'].transform(lambda x: x.rolling(window=21, min_periods=1).mean())
df['moving_avg_28'] = df.groupby('Symbol')['Close'].transform(lambda x: x.rolling(window=28, min_periods=1).mean())

# Select  columns
result = df[['Date', 'Symbol', 'Close', 'moving_avg_7', 'moving_avg_14', 'moving_avg_21', 'moving_avg_28']]
print(result)
path1 = r'C:\d\loop\stock_project\moving_avg.csv'
result.to_csv(path1, index=False)

# Establish database connection
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='root123',
    database='stock_data'
)

with connection.cursor() as cursor:
    for index, row in result.iterrows():
        sql = """
        INSERT INTO stock_moving_avg (Date, Symbol, Close, moving_avg_7, moving_avg_14, moving_avg_21, moving_avg_28)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (
            row['Date'],
            row['Symbol'],
            row['Close'],
            row['moving_avg_7'],
            row['moving_avg_14'],
            row['moving_avg_21'],
            row['moving_avg_28']
        ))
    connection.commit()

print("Data inserted into stock_moving_avg_table.")

connection.close()