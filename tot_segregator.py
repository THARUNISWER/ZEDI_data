import sqlite3
from datetime import datetime
import os
import pytz

# Connecting with sql database (database auto create if not present)
connection = sqlite3.connect("ZEDI_data.db")

node_path = input("Enter path to node: ")

# the final directories where segregated excel files need to be created (used in extractor)
directory_1s = input("Enter path for directory of 1s_sorted_data for this node: ")
directory_5m = input("Enter path for directory of 5m_sorted_data for this node: ")

# the path to the error storing text file where errors are loaded
error_file_path = input("Enter error file path: ")
if os.path.exists(error_file_path) is False:
    print("Enter valid text file path next time")
    quit()

err = open(error_file_path, "w")

# Indian timezone for epoch
tz_GMT = pytz.timezone("UTC")

crsr = connection.cursor()

# commands for creating sql table for 1s data
crsr.execute("DROP TABLE IF EXISTS ZEDI_1s")
sql_command = "CREATE TABLE ZEDI_1s(" \
              "date_and_time smalldatetime PRIMARY KEY," \
              "current_R decimal(9,3)," \
              "current_Y decimal(9,3)," \
              "current_B decimal(9,3)," \
              "VOLTAGE_R decimal(9,3)," \
              "VOLTAGE_Y decimal(9,3)," \
              "VOLTAGE_B decimal(9,3)," \
              "POWER_R decimal(9,3)," \
              "POWER_Y decimal(9,3)," \
              "POWER_B decimal(9,3)," \
              "POWER_TOT decimal(9,3)," \
              "FREQ_R decimal(9,3)," \
              "FREQ_Y decimal(9,3)," \
              "FREQ_B decimal(9,3))"
crsr.execute(sql_command)

# commands for creating sql table for 5m data
crsr.execute("DROP TABLE IF EXISTS ZEDI_5m")
sql_command = "CREATE TABLE ZEDI_5m(" \
              "date_and_time smalldatetime PRIMARY KEY," \
              "temperature decimal(9,3)," \
              "humidity decimal(9,3)," \
              "pressure decimal(9,3)," \
              "aqi decimal(9,3)," \
              "co2 decimal(9,3)," \
              "pir1 decimal(9,3)," \
              "pir2 decimal(9,3));"

crsr.execute(sql_command)

format = "%Y-%m-%d %H:%M:%S"
DELIM = "|"
val = None


curr_file_name = ""
curr_line_no = ""
# function for error correction
def conv(var):
    try:
        ans = float(var)
    except:
        err.write("Curr file: " + curr_file_name + ", ")
        err.write("Curr_line: " + curr_line_no + ", ")
        err.write("Erraneous data in field: " + var + "\n")
        ans = val
    return ans


# function to calculate power precisely based on voltage
def condition(current, voltage):
    power = 0.0
    if voltage > 100:
        power = current*voltage
    return power


# a function that takes in the dataframe_id and all values in a raw line and segregates data accordingly
def updation(dataframe_id, values):
    if dataframe_id == 1:  # for current_r and voltage_r dataframe_id is 1
        i = 6  # actual repeating data starts from index 6 in line without delimiter
        while i < len(values):
            try:
                epoch = int(values[i])
            except:
                err.write("Curr file: " + curr_file_name + ", ")
                err.write("Curr_line: " + curr_line_no + ", ")
                err.write("Invalid epoch data: " + values[i] + "\n")
                i = i+3
                continue
            curr_date_time = datetime.fromtimestamp(epoch, tz_GMT)  # epoch to datetime object converter
            # conv to convert to float as well as check error
            voltage = conv(values[i-1])
            current = conv(values[i-2])
            power = 0.0
            # power is calculated
            if current is not None and voltage is not None:
                power = condition(current, voltage)
            # checking if that datetime is already present in the database
            crsr.execute("SELECT EXISTS(SELECT * from ZEDI_1s WHERE date_and_time = ?)", [str(curr_date_time.strftime(format))])
            out = str(crsr.fetchall())
            if out != "[(0,)]":
                # if datetime is already present update existing row to include new data
                crsr.execute("UPDATE ZEDI_1s SET current_R = ?, VOLTAGE_R = ?,POWER_R = ?  WHERE date_and_time= ?", (current,
                                                                                                        voltage, power,
                                                                                            str(curr_date_time.strftime(format))))
            else:
                # if datetime is not present insert a new row with that datetime and data
                crsr.execute('INSERT INTO ZEDI_1s VALUES (?, ?, ?, ?, ?,'
                             '?, ?, ?, ?, ?, ?, ?, ?, ?)',
                             (str(curr_date_time.strftime(format)), current, val, val, voltage, val, val, power, val, val, val, val, val, val))

            i = i + 3

    # repeat same format as in dataframe_id == 1 for rest of code
    elif dataframe_id == 2:
        i = 6
        while i < len(values):
            try:
                epoch = int(values[i])
            except:
                err.write("Curr file: " + curr_file_name + ", ")
                err.write("Curr_line: " + curr_line_no + ", ")
                err.write("Invalid epoch data: " + values[i] + "\n")
                i = i+3
                continue
            curr_date_time = datetime.fromtimestamp(epoch, tz_GMT)
            voltage = conv(values[i - 1])
            current = conv(values[i - 2])
            power = 0.0
            if current is not None and voltage is not None:
                power = condition(current, voltage)
            crsr.execute("SELECT EXISTS(SELECT * from ZEDI_1s WHERE date_and_time = ?)",
                         [str(curr_date_time.strftime(format))])
            out = str(crsr.fetchall())
            if out != "[(0,)]":
                crsr.execute("UPDATE ZEDI_1s SET current_Y = ?, VOLTAGE_Y = ?, POWER_Y = ? WHERE date_and_time= ?", (current,
                                                                                                        voltage,
                                                                                                        power,
                                                                                                        str(curr_date_time.
                                                                                                        strftime(
                                                                                                        format))))
            else:
                crsr.execute('INSERT INTO ZEDI_1s VALUES (?, ?, ?, ?, ?,'
                             '?, ?, ?, ?, ?, ?, ?, ?, ?)',
                             (str(curr_date_time.strftime(format)), val, current, val, val, voltage, val, val, power,
                              val, val, val, val, val))
            i = i + 3
    elif dataframe_id == 3:
        i = 6
        while i < len(values):
            try:
                epoch = int(values[i])
            except:
                err.write("Curr file: " + curr_file_name + ", ")
                err.write("Curr_line: " + curr_line_no + ", ")
                err.write("Invalid epoch data: " + values[i] + "\n")
                i = i+3
                continue
            curr_date_time = datetime.fromtimestamp(epoch, tz_GMT)  # epoch to datetime object converter
            voltage = conv(values[i - 1])
            current = conv(values[i - 2])
            power = 0.0
            if current is not None and voltage is not None:
                power = condition(current, voltage)
            crsr.execute("SELECT EXISTS(SELECT * from ZEDI_1s WHERE date_and_time = ?)",
                         [str(curr_date_time.strftime(format))])
            if str(crsr.fetchall()) != "[(0,)]":
                crsr.execute("UPDATE ZEDI_1s SET current_B = ?, VOLTAGE_B = ?, POWER_B = ? WHERE date_and_time= ?", (current,
                                                                                                    voltage,
                                                                                                    power,
                                                                                                    str(
                                                                                                        curr_date_time.strftime(
                                                                                                            format))))
            else:
                crsr.execute('INSERT INTO ZEDI_1s VALUES (?, ?, ?, ?, ?,'
                             '?, ?, ?, ?, ?, ?, ?, ?, ?)',
                             (str(curr_date_time.strftime(format)), val, val, current, val, val, voltage, val, val,
                              power, val, val, val, val))
            i = i + 3
    elif dataframe_id == 4:
        i = 5
        while i < len(values):
            try:
                epoch = int(values[i])
            except:
                err.write("Curr file: " + curr_file_name + ", ")
                err.write("Curr_line: " + curr_line_no + ", ")
                err.write("Invalid epoch data: " + values[i] + "\n")
                i = i+2
                continue
            curr_date_time = datetime.fromtimestamp(epoch, tz_GMT)
            frequency = conv(values[i - 1])

            crsr.execute("SELECT EXISTS(SELECT * from ZEDI_1s WHERE date_and_time = ?)",
                         [str(curr_date_time.strftime(format))])
            if str(crsr.fetchall()) != "[(0,)]":
                crsr.execute("UPDATE ZEDI_1s SET freq_R = ? WHERE date_and_time= ?", (frequency,
                                                                                     str(
                                                                                        curr_date_time.strftime(
                                                                                         format))))
            else:
                crsr.execute('INSERT INTO ZEDI_1s VALUES (?, ?, ?, ?, ?,'
                             '?, ?, ?, ?, ?, ?, ?, ?, ?)',
                             (str(curr_date_time.strftime(format)), val, val, val, val, val, val,val, val ,val, val, frequency, val, val))

            i = i + 2
    elif dataframe_id == 5:
        i = 5
        while i < len(values):
            try:
                epoch = int(values[i])
            except:
                err.write("Curr file: " + curr_file_name + ", ")
                err.write("Curr_line: " + curr_line_no + ", ")
                err.write("Invalid epoch data: " + values[i] + "\n")
                i = i+2
                continue
            curr_date_time = datetime.fromtimestamp(epoch, tz_GMT)
            frequency = conv(values[i - 1])
            crsr.execute("SELECT EXISTS(SELECT * from ZEDI_1s WHERE date_and_time = ?)",
                         [str(curr_date_time.strftime(format))])
            if str(crsr.fetchall()) != "[(0,)]":
                crsr.execute("UPDATE ZEDI_1s SET freq_Y = ? WHERE date_and_time= ?", (frequency,
                                                                                  str(
                                                                                      curr_date_time.strftime(
                                                                                          format))))
            else:
                crsr.execute('INSERT INTO ZEDI_1s VALUES (?, ?, ?, ?, ?,'
                             '?, ?, ?, ?, ?, ?, ?, ?, ?)',
                             (str(curr_date_time.strftime(format)), val, val, val, val, val, val, val, val, val, val,
                              val, frequency, val))

            i = i + 2
    elif dataframe_id == 6:
        i = 5
        while i < len(values):
            try:
                epoch = int(values[i])
            except:
                err.write("Curr file: " + curr_file_name + ", ")
                err.write("Curr_line: " + curr_line_no + ", ")
                err.write("Invalid epoch data: " + values[i] + "\n")
                i = i + 2
                continue
            curr_date_time = datetime.fromtimestamp(epoch, tz_GMT)
            frequency = conv(values[i - 1])
            crsr.execute("SELECT EXISTS(SELECT * from ZEDI_1s WHERE date_and_time = ?)",
                         [str(curr_date_time.strftime(format))])
            if str(crsr.fetchall()) != "[(0,)]":
                crsr.execute("UPDATE ZEDI_1s SET freq_B = ? WHERE date_and_time= ?", (frequency,
                                                                                  str(
                                                                                      curr_date_time.strftime(
                                                                                          format))))
            else:
                crsr.execute('INSERT INTO ZEDI_1s VALUES (?, ?, ?, ?, ?,'
                             '?, ?, ?, ?, ?, ?, ?, ?, ?)',
                             (str(curr_date_time.strftime(format)), val, val, val, val, val, val, val, val, val, val,
                              val, val, frequency))

            i = i + 2
    elif dataframe_id == 7:
        i = len(values) - 1  # starting from last in case of 7 as light_level, ultrasonic not needed
        try:
            epoch = int(values[i])
        except:
            err.write("Curr file: " + curr_file_name + ", ")
            err.write("Curr_line: " + curr_line_no + ", ")
            err.write("Invalid epoch data: " + values[i] + "\n")
            return
        curr_date_time = datetime.fromtimestamp(epoch, tz_GMT)
        co2 = conv(values[i - 4])
        pir2 = conv(values[i-5])
        pir1 = conv(values[i - 6])
        aqi = conv(values[i - 7])
        bme680_pressure = conv(values[i - 8])
        bme680_humidity = conv(values[i - 9])
        bme680_temp = conv(values[i - 10])
        bme280_pressure = conv(values[i - 11])
        bme280_humidity = conv(values[i - 12])
        bme280_temp = conv(values[i - 13])

        temperature = bme280_temp
        pressure = bme280_pressure
        humidity = bme280_humidity

        # if bme_280 values are 0 i.e missing we go with bme_680 values in the table
        if bme280_temp == 0 or bme280_temp == val:
            temperature = bme680_temp
        if bme280_pressure == 0 or bme280_pressure == val:
            pressure = bme680_pressure
        if bme280_humidity == 0 or bme280_humidity == val:
            humidity = bme680_humidity

        crsr.execute("SELECT EXISTS(SELECT * from ZEDI_5m WHERE date_and_time = ?)",
                     [str(curr_date_time.strftime(format))])
        out = str(crsr.fetchall())
        if out != "[(0,)]":
            crsr.execute("UPDATE ZEDI_5m SET temperature = ?, humidity = ?, pressure = ?, aqi = ?, co2 = ?,"
                         " pir1 = ?, pir2 = ? WHERE date_and_time = ?",
                         (temperature, humidity, pressure, aqi, co2, pir1, pir2, str(curr_date_time.strftime(format))))
        else:
            crsr.execute('INSERT INTO ZEDI_5m VALUES (?, ?, ?, ?, ?,'
                         '?, ?, ?)',
                         (str(curr_date_time.strftime(format)), temperature, humidity, pressure, aqi, co2, pir1, pir2))
    else:
        err.write("Curr file: " + curr_file_name + ", ")
        err.write("Curr_line: " + curr_line_no + ", ")
        err.write("Invalid dataframe_id(i.e a break in line): " + str(dataframe_id) + "\n")


# iterating through all days in the node i.e folders within directory
for root, subdirectories, files in os.walk(node_path):
    print("Files starting to iterate..")
    # iterating through all files in that day i.e files in folders
    for filename in files:
        curr_file_name = os.path.join(root, filename)
        print("Started: " + os.path.join(root, filename))
        file = open(os.path.join(root, filename))
        # iterating through all lines in the file
        line_no = 1
        values = []
        flag = 0
        for line in file:
            curr_line_no = str(line_no)
            line = line.strip()
            values.extend(line.split(DELIM))  # removing delimiter and extracting data in array for strings
            try:
                ID = int(values[3])
            except:
                values.clear()
                continue

            # the code that deals with line breaks if present
            if 3 >= ID >= 1:
                if len(values) == 184:
                    updation(ID, values)
                elif len(values) < 184 and flag == 0:
                    err.write("Curr file: " + curr_file_name + ", ")
                    err.write("Curr_line: " + curr_line_no + ", ")
                    err.write("Unexpected line break" + "\n")
                    flag = 1
                    continue
                else:
                    err.write("Curr file: " + curr_file_name + ", ")
                    err.write("Curr_line: " + curr_line_no + ", ")
                    err.write("Unexpected line break" + "\n")
            elif 6 >= ID >= 4:
                if len(values) == 124:
                    updation(ID, values)
                elif len(values) < 124 and flag == 0:
                    err.write("Curr file: " + curr_file_name + ", ")
                    err.write("Curr_line: " + curr_line_no + ", ")
                    err.write("Unexpected line break" + "\n")
                    flag = 1
                    continue
                else:
                    err.write("Curr file: " + curr_file_name + ", ")
                    err.write("Curr_line: " + curr_line_no + ", ")
                    err.write("Unexpected line break" + "\n")
            elif ID == 7:
                updation(ID, values)
            else:
                err.write("Curr file: " + curr_file_name + ", ")
                err.write("Curr_line: " + curr_line_no + ", ")
                err.write("Invalid dataframe_id(i.e a break in line): " + str(ID) + "\n")

            flag = 0
            line_no = line_no+1
            values.clear()
        print("Ended: " + os.path.join(root, filename))
        file.close()
crsr.execute("UPDATE ZEDI_1s SET POWER_TOT = POWER_R + POWER_Y + POWER_B")

connection.commit() # all changes that this code does are commited to the database

# retrieving maximum and minimum date in database for further processing in extractor
display_format = "[('%Y-%m-%d %H:%M:%S',)]"

min_date_time_1s = datetime(1974,2,12,0,0,0)
max_date_time_1s = datetime(1974,2,12,0,0,0)
min_date_time_5m = datetime(1974,2,12,0,0,0)
max_date_time_5m = datetime(1974,2,12,0,0,0)
try:
    crsr.execute("SELECT MIN(date_and_time) FROM ZEDI_1s")
    min_date_time_1s = datetime.strptime(str(crsr.fetchall()), display_format)
    crsr.execute("SELECT MAX(date_and_time) FROM ZEDI_1s")
    max_date_time_1s = datetime.strptime(str(crsr.fetchall()), display_format)
except:
    print("No 1s_data")
try:
    crsr.execute("SELECT MIN(date_and_time) FROM ZEDI_5m")
    min_date_time_5m = datetime.strptime(str(crsr.fetchall()), display_format)
    crsr.execute("SELECT MAX(date_and_time) FROM ZEDI_5m")
    max_date_time_5m = datetime.strptime(str(crsr.fetchall()), display_format)
except:
    print("No 5m_data")

connection.close()

