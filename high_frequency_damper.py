import os
import shutil

#node_path = input("Enter node/folder path: ")

DELIM = ','


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


def draw_line(data, clip_start, clip_end):
    new_data = data
    for i in range(0, len(clip_start)):

        slope = (data[clip_end[i]] - data[clip_start[i]])/(clip_end[i] - clip_start[i])
        for j in range(clip_start[i] + 1, clip_end[i]):
            new_data[j] = (data[clip_start[i]] + slope*(j-clip_start[i]))
    return new_data


def moving_ave_osc(data):
    i = 1
    moving_ave = []
    clip_start = []
    clip_end = []
    for i in range(0,len(data)):
        print(data[i], end = " ")
    #print()
    for i in range(1, len(data)):
        if isNaN(data[i]) or isNaN(data[i-1]):
            continue
        ave_2_point = (data[i] + data[i-1])/2
        #print(ave_2_point, end = " ")
        moving_ave.append(ave_2_point)

    #print()
    j = 0
    i = 1
    while i<len(data):
        if isNaN(data[i]):
            i = i + 1
            continue
        diff = abs(data[i] - moving_ave[j])
        #print(diff, end = "  ")
        if diff > 10:
            clip_start.append(i - 1)
            k = i
            while diff > 10 and k < len(data):
                if isNaN(data[k]):
                    k = k + 1
                    continue
                diff = abs(data[k] - moving_ave[j])
                print(diff, end = " ")
                k = k + 1
                j = j + 1
            if k == len(data):
                k = k - 1
            clip_end.append(k)
            i = k
        j = j + 1
        i = i + 1

    # print()
    # print(len(moving_ave))
    # print(len(data))
    # print(len(clip_start))
    # print(len(clip_end))
    # print(j)
    # print(i)

    new_data = draw_line(data, clip_start, clip_end)
    return new_data


def remove_osc(all_lines):
    temperature = []
    humidity = []
    pressure = []
    aqi = []
    co2 = []
    pir1 = []
    pir2 = []
    epoch = []
    for lin in all_lines:
        line = lin.strip()  # removes '\n' from end of line
        values = line.split(DELIM)
        epoch.append(values[0])
        temperature.append(conv(values[1]))
        humidity.append(conv(values[2]))
        pressure.append(conv(values[3]))
        aqi.append(conv(values[4]))
        co2.append(conv(values[5]))
        pir1.append(conv(values[6]))
        pir2.append(conv(values[7]))

    new_aqi = moving_ave_osc(aqi)

    new_lines = []
    # lines stitching
    for i in range(len(all_lines)):
        line = epoch[i] + DELIM + re_conv(temperature[i]) + DELIM + re_conv(humidity[i]) + DELIM + re_conv(pressure[i]) + DELIM + re_conv(
               new_aqi[i]) + DELIM + re_conv(co2[i]) + DELIM + re_conv(pir1[i]) + DELIM + re_conv(pir2[i])
        new_lines.append(line)
    return new_lines


# for root, subdirectories, files in os.walk(node_path):
#     print("Files starting to cleanse..")
#     # iterating through all files in that day i.e files in folders
   # for filename in files:

root = "C:\\Users\\tharu\Downloads"
filename = "test_file.csv"
print("Started: " + os.path.join(root, filename))
file = open(os.path.join(root, filename))
if os.path.exists("replacer.csv"):  # a file to temporarily store the new lines
    os.remove("replacer.csv")
new_file = open("replacer.csv", "a")
file_lines = file.readlines()
new_file.write(file_lines[0])  # header for all columns remains same
file_lines = file_lines[1:]
new_file_lines = remove_osc(file_lines)
for line in new_file_lines:
    lin = line + "\n"
    new_file.write(lin)
new_file.close()
file.close()
# shutil.copyfile("replacer.csv", os.path.join(root, filename))
# copies contents of replacer to currently open file
print("Ended: " + os.path.join(root, filename))