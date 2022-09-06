import os
import shutil

node_path = input("Enter node path: ")

DELIM = ','

range_top = float(input("Enter upper bound for voltage: "))
range_per = float(input("Enter percentage above upper bound that is allowed: "))
des_freq = int(input("Enter desired frequency of data to be considered as legit: "))  # the desired frequency of cleansing


# to add NaN as if they are 0's
def add_nan(var):
    ans = var
    if var == float("NaN"):
        ans = 0.0
    return ans


# code for error correction while conversion to float i.e there could be blank fields
def conv(var):
    try:
        ans = float(var)
    except:
        ans = float("NaN")
        print(end="")
    return ans


# to check if a value is NaN
def isNaN(num):
    return num != num


# function to make string blank if value is none
def re_conv(var):
    ans = ""
    if not isNaN(var):
        ans = str(var)
    return ans


# function that takes in test_data and caps data that satisfies a particular condition
def cont_series_cleanse(test_data):
    # start stores the starting index of data that is between range_top and accepted_top
    start = []
    # end stores the ending index of data that is between range_top and accepted_top
    end = []
    # accepted_top is range_top with percentage added
    accepted_top = range_top * (range_per + 100) / 100
    for i in range(len(test_data)):
        if not isNaN(test_data[i]):
            if range_top < test_data[i] < accepted_top:
                # finding out continuous series of data that is between accepted_top and range_top
                if len(end) > 0 and end[len(end) - 1] == i - 1:
                    end[len(end) - 1] += 1
                else:
                    end.append(i)
                    start.append(i)
                # if data exceeds accepted_top cap it
            elif test_data[i] >= accepted_top:
                test_data[i] = range_top
    # checking if continuous data between range_top and accepted_top is less than desired frequency for capping
    for i in range(len(end)):
        if end[i] - start[i] < des_freq:
            for j in range(start[i], end[i] + 1):
                test_data[j] = range_top

    return test_data

# function that breaks each line, cleanses the voltage, then stiches all back together
def cleanse_1s(all_lines):
    current_r = []
    current_y = []
    current_b = []
    voltage_r = []
    voltage_y = []
    voltage_b = []
    frequency_r = []
    frequency_y = []
    frequency_b = []
    epoch = []
    for lin in all_lines:
        line = lin.strip()  # removes '\n' from end of line
        values = line.split(DELIM)
        epoch.append(values[0])
        current_r.append(values[1])
        current_y.append(values[2])
        current_b.append(values[3])
        voltage_r.append(conv(values[4]))
        voltage_y.append(conv(values[5]))
        voltage_b.append(conv(values[6]))
        frequency_r.append(values[7])
        frequency_y.append(values[8])
        frequency_b.append(values[9])

    # voltage sent for cleansing
    new_voltage_r = cont_series_cleanse(voltage_r)
    new_voltage_y = cont_series_cleanse(voltage_y)
    new_voltage_b = cont_series_cleanse(voltage_b)

    new_lines = []
    # lines stitching
    for i in range(len(all_lines)):
        power_r = conv(current_r[i])*new_voltage_r[i]
        power_y = conv(current_y[i])*new_voltage_y[i]
        power_b = conv(current_b[i])*new_voltage_b[i]
        power_tot = add_nan(power_r) + add_nan(power_y) + add_nan(power_b)
        line = epoch[i] + DELIM + current_r[i] + DELIM + current_y[i] + DELIM + current_b[i] + DELIM + re_conv(
               new_voltage_r[i]) + DELIM + re_conv(new_voltage_y[i]) + DELIM + re_conv(new_voltage_b[i]) + DELIM \
               + frequency_r[i] + DELIM + frequency_y[i] + DELIM + frequency_b[i] + DELIM + re_conv(power_r) \
               + DELIM + re_conv(power_y) + DELIM + re_conv(power_b) + DELIM + re_conv(power_tot)
        new_lines.append(line)
    return new_lines


for root, subdirectories, files in os.walk(node_path):
    print("Files starting to cleanse..")
    # iterating through all files in that day i.e files in folders
    for filename in files:
        print("Started: " + os.path.join(root, filename))
        file = open(os.path.join(root, filename))
        if os.path.exists("replacer.csv"):  # a file to temporarily store the new lines
            os.remove("replacer.csv")
        new_file = open("replacer.csv", "a")
        file_lines = file.readlines()
        new_file.write(file_lines[0])  # header for all columns remains same
        file_lines = file_lines[1:]
        new_file_lines = cleanse_1s(file_lines)
        for line in new_file_lines:
            lin = line + "\n"
            new_file.write(lin)
        new_file.close()
        file.close()
        shutil.copyfile("replacer.csv", os.path.join(root, filename))
        # copies contents of replacer to currently open file
        print("Ended: " + os.path.join(root, filename))