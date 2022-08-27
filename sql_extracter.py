from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import calendar

import tot_segregator  # importing tot_segregator to segregate first and then run extractor

end_date_time_1s = tot_segregator.max_date_time_1s
start_date_time_1s = tot_segregator.min_date_time_1s
end_date_time_5m = tot_segregator.max_date_time_5m
start_date_time_5m = tot_segregator.min_date_time_5m


# function to add one month for iteration purpose in 5m data
def add_months(sourcedate, months):
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year, month)[1])
    return datetime(year, month, day, sourcedate.hour, sourcedate.minute, sourcedate.second)


# function to find last day of any month
def last_day_of_month(any_day):
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return next_month - timedelta(days=next_month.day)


conn = sqlite3.connect("ZEDI_data.db")
FILE_NAME_FORMAT_1S = "%d-%m-%y"

print("Extraction started...")
# iterating over all days between start_date_time and end_date_time
curr_date_time = start_date_time_1s
delta = timedelta(days=1)  # used for incrementing current date by 1 day
while curr_date_time <= end_date_time_1s:
    next_date_time = curr_date_time + delta
    start_date_time = curr_date_time.replace(hour=0, minute=0, second=0)  # start_date is year:month:day 00:00:00
    end_date_time = datetime(curr_date_time.year, curr_date_time.month, curr_date_time.day, 23, 59, 59)

    # end_date is year:month:day 11:59:59
    sql_query = pd.read_sql_query('''
                                  SELECT * FROM ZEDI_1s WHERE date_and_time BETWEEN ? AND ?
                                  ''', conn, params=[start_date_time, end_date_time])
    # here, the 'conn' is the variable that contains your database connection information from step 2

    df = pd.DataFrame(sql_query)
    if df.empty:  # if there exists no data in that date ,don't create a excel file
        curr_date_time += delta
        continue
    filename = tot_segregator.directory_1s + "\\" + curr_date_time.strftime(FILE_NAME_FORMAT_1S) + ".csv"
    print("Creating: " + filename)
    df.to_csv(filename, index=False)  # dataframe is uploaded onto csv
    curr_date_time += delta

FILE_NAME_FORMAT_5m = "%m-%y"

curr_date_time = start_date_time_5m

while curr_date_time <= end_date_time_5m:
    next_date_time = add_months(curr_date_time, 1)
    start_date_time = curr_date_time.replace(day=1)  # start_date is year:month:1 00:00:00
    start_date_time = start_date_time.replace(hour=0, minute=0, second=0)
    end_date_time = last_day_of_month(curr_date_time)  # end_date is year:month:last_day_of_month 11:59:59
    end_date_time = datetime(end_date_time.year, end_date_time.month, end_date_time.day, 23, 59, 59)
    sql_query = pd.read_sql_query('''
                                  SELECT * FROM ZEDI_5m WHERE date_and_time BETWEEN ? AND ?
                                  ''', conn, params=[start_date_time, end_date_time])
    # here, the 'conn' is the variable that contains your database connection information

    df = pd.DataFrame(sql_query)
    if df.empty:
        curr_date_time = next_date_time
        continue
    filename = tot_segregator.directory_5m + "\\" + curr_date_time.strftime(FILE_NAME_FORMAT_5m) + ".csv"
    print("Creating: " + filename)
    df.to_csv(filename, index=False)
    curr_date_time = next_date_time

conn.close()
