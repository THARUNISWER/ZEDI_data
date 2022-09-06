import os
import shutil

DELIM = ","

node_path = input("Enter node/folder path: ")
# testing
# temp_max = float(35)
# temp_min = float(-3)
# hum_max = float(100)
# hum_min = float(20)
# pres_max = float(100000)
# pres_min = float(75000)
# aqi_max = float(150)
# aqi_min = float(20)
# co2_max = float(2000)
# co2_min = float(250)
# pir1_max = float(100000)
# pir1_min = float(0)
# pir2_max = float(100000)
# pir2_min = float(0)


temp_max = float(input("temp_max: "))
temp_min = float(input("temp_min: "))
hum_max = float(input("hum_max: "))
hum_min = float(input("hum_min: "))
pres_max = float(input("pres_max: "))
pres_min = float(input("pres_min: "))
aqi_max = float(input("aqi_max: "))
aqi_min = float(input("aqi_min: "))
co2_max = float(input("co2_max: "))
co2_min = float(input("co2_min: "))

# pir1_max = float(input("pir1_max: "))
# pir1_min = float(input("pir1_min: "))
# pir2_max = float(input("pir2_max: "))
# pir2_min = float(input("pir2_min: "))


# to check if a value is NaN
def isNaN(num):
    return num != num


# code for error correction while conversion to float i.e there could be blank fields
def conv(var):
    try:
        ans = float(var)
    except:
        ans = float("NaN")
        print(end="")
    return ans


# function to make string blank if value is none
def re_conv(var):
    ans = ""
    if not isNaN(var):
        ans = str(var)
    return ans


# function that breaks each line, cleanses the voltage, then stiches all back together
def cleanse_5m(file):
    new_file = open("replacer.csv", "a")
    ini = file.readline()
    new_file.write(ini)
    for lin in file:
        line = lin.strip()  # removes '\n' from end of line
        values = line.split(DELIM)
        epoch = values[0]
        temperature = conv(values[1])
        humidity = conv(values[2])
        pressure = conv(values[3])
        aqi = conv(values[4])
        co2 = conv(values[5])
        pir1 = conv(values[6])
        pir2 = conv(values[7])

        # checking if values are in range otherwise changing to NaN
        if not isNaN(temperature):
            if temperature > temp_max:
                temperature = float("NaN")
            elif temperature < temp_min:
                temperature = float("NaN")

        if not isNaN(humidity):
            if humidity > hum_max:
                humidity = float("NaN")
            elif humidity < hum_min:
                humidity = float("NaN")

        if not isNaN(pressure):
            if pressure > pres_max:
                pressure = float("NaN")
            elif pressure < pres_min:
                pressure = float("NaN")

        if not isNaN(aqi):
            if aqi > aqi_max:
                aqi = float("NaN")
            elif aqi < aqi_min:
                aqi = float("NaN")

        if not isNaN(co2):
            if co2 > co2_max:
                co2 = float("NaN")
            elif co2 < co2_min:
                co2 = float("NaN")

        # if not isNaN(pir1):
        #     if pir1 > pir1_max:
        #         pir1 = float("NaN")
        #     elif pir1 < pir1_min:
        #         pir1 = float("NaN")
        #
        # if not isNaN(co2):
        #     if pir2 > pir2_max:
        #         pir2 = float("NaN")
        #     elif pir2 < pir2_min:
        #         pir2 = float("NaN")

        new_line = epoch + DELIM + re_conv(temperature) + DELIM + re_conv(humidity) + DELIM + re_conv(pressure) \
                   + DELIM + re_conv(aqi) + DELIM + re_conv(co2) + DELIM + re_conv(pir1) + DELIM + re_conv(pir2) + "\n"
        new_file.write(new_line)

    new_file.close()


for root, subdirectories, files in os.walk(node_path):
    print("Files starting to cleanse..")
    # iterating through all files in that day i.e files in folders
    for filename in files:
        print("Started: " + os.path.join(root, filename))
        file = open(os.path.join(root, filename))
        if os.path.exists("replacer.csv"):  # a file to temporarily store the new lines
            os.remove("replacer.csv")
        cleanse_5m(file)
        file.close()
        shutil.copyfile("replacer.csv", os.path.join(root, filename))
        # copies contents of replacer to currently open file
        print("Ended: " + os.path.join(root, filename))
