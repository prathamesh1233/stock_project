from datetime import datetime
import pymysql
import pandas as pd

import yfinance as yf

def create_calendar_dimension(start_date='2023-01-01', end_date='2024-12-31'):
    data1 = {
        'Date': pd.date_range(start=start_date, end=end_date, freq='D')
    }
    df = pd.DataFrame(data1)

    df['DateKey'] = df['Date'].dt.strftime('%Y%m%d')
    df['Day'] = df['Date'].dt.day
    df['Month'] = df['Date'].dt.month
    df['MonthName'] = df['Date'].dt.month_name()
    df['Quarter'] = df['Date'].dt.quarter
    df['QuarterName'] = df['Date'].dt.to_period('Q').astype(str)
    df['Year'] = df['Date'].dt.year
    df['DayOfWeek'] = df['Date'].dt.dayofweek + 1  # Monday=1, Sunday=7
    df['DayOfWeekName'] = df['Date'].dt.day_name()
    df['IsWeekend'] = df['DayOfWeek'].isin([6, 7])  # Saturday=6, Sunday=7
    df['WeekOfYear'] = df['Date'].dt.isocalendar().week

    # Calculate FirstDayOfMonth, LastDayOfMonth, EndOfWeek
    df['FirstDayOfMonth'] = df['Date'].dt.to_period('M').dt.start_time
    df['LastDayOfMonth'] = df['Date'].dt.to_period('M').dt.end_time
    df['EndOfWeek'] = df['Date'] + pd.offsets.Week(weekday=6)  # End of week (Sunday)

    # Format dates to YYYY-MM-DD
    for col in ['Date', 'FirstDayOfMonth', 'LastDayOfMonth', 'EndOfWeek']:
        df[col] = df[col].dt.strftime('%Y-%m-%d')

    path = r'C:\d\loop\stock_project\calendar_dimension.csv'
    df.to_csv(path, index=False)

    return df  # Return the DataFrame


def create_holiday_dataframe(file_path):
    df_holidays = pd.read_csv(file_path)
    df_holidays.columns = ['SR. NO.', 'Date', 'Day', 'Occasion']

    # Clean the Date column
    df_holidays['Date'] = df_holidays['Date'].str.strip()
    df_holidays = df_holidays[df_holidays['Date'] != '']
    df_holidays['Date'] = pd.to_datetime(df_holidays['Date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
    df_holidays = df_holidays.dropna(subset=['Date'])

    # print("\ndf_holidays DataFrame:")
    # print(df_holidays)
    return df_holidays  # Return the DataFrame


def get_non_holiday_non_weekend_days(df, df_holidays):
    # Perform a left merge to identify non-holiday dates
    merged_df = pd.merge(df, df_holidays[['Date']], on='Date', how='left', indicator=True)
    non_holiday_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])

    # Filter to remove Saturday (6) and Sunday (7)
    non_weekend_df = non_holiday_df[~non_holiday_df['DayOfWeek'].isin([6, 7])]

    # print("\nNon-Holiday Non-Weekend DataFrame:")
    # print(non_weekend_df)
    return non_weekend_df  # Return the filtered DataFrame


# Main execution
calendar_df = create_calendar_dimension()
holidays_df = create_holiday_dataframe(r'C:\d\loop\stock_project\HolidaycalenderData.csv')
non_holiday_non_weekend_df = get_non_holiday_non_weekend_days(calendar_df, holidays_df)

# Save the result to a CSV file
path1 = r'C:\d\loop\stock_project\trading_day.csv'
non_holiday_non_weekend_df.to_csv(path1, index=False)

# Define your database connection parameters
output_path = r'C:\d\loop\stock_project\trading_day.csv'
data = pd.read_csv(output_path)

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

# Read back the trading days from the CSV
trading_days = []
with open(path1) as f:
    next(f)
    for line in f:
        line = line.strip()
        if line:
            Date = line.split(",")[0]
            trading_days.append(Date)
#print(trading_days)

path2 = r'C:\d\loop\stock_project\EQUITY_L.csv'
symbol_list = []

with open(path2) as f:
    next(f)
    for line in f:
        line = line.strip()
        if line:
            symbol = line.split(",")[0]
            symbol_list.append(symbol)
#print(symbol_list)
# sym = symbol_list[0:10]
#print(sym)
# date range
start_date = '2023-01-01'
#end_date = '2024-12-31'
end_date = datetime.today().date()

trading_data = pd.DataFrame()
empty_symbols = []
for symbol in symbol_list:
    try:
        data = yf.download(symbol+".NS", start=start_date, end=end_date)
        #print(symbol,start_date,end_date)
        #print(data)
        if data.empty:
            print(f"No data for symbol: {symbol}")
            empty_symbols.append(symbol)
            continue
        data.index = pd.to_datetime(data.index)
        data = data[data.index.strftime('%Y-%m-%d').isin(trading_days)]
        data['Turnover'] = (data['Close'] * data['Volume']) / 10 ** 7    # Turnover in crores
        data['Symbol'] = symbol
        trading_data = pd.concat([trading_data, data])

    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")

trading_data.reset_index(inplace=True)
# print(trading_data)
output_path = r'C:\d\loop\stock_project\final_trading_data.csv'
trading_data.to_csv(output_path, index=False)
print(trading_data.columns.tolist())

num_rows, num_columns = trading_data.shape
print(f"Number of rows: {num_rows}")
print(f"Number of columns: {num_columns}")
print("delisted symbol list:", empty_symbols)
