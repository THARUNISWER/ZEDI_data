import os
from fastdtw import fastdtw
import numpy as np

DELIM = ','
backup = "backup.csv"
if not os.path.exists(backup):
    print("First run DTW extractor.py")
    exit(0)
backupfile = open(backup)

# add fridge file's name here
target_file_fridge = "C:\\Users\\tharu\\Downloads\\Fridge\\DATALOG.csv"
output_file_fridge = "fridge_dtw.csv"
target_file_geyser = ""
target_file_invac = ""


def dtw(s, t):
    print(s)
    print(t)
    n, m = len(s), len(t)
    dtw_matrix = np.zeros((n + 1, m + 1))
    for i in range(n + 1):
        for j in range(m + 1):
            dtw_matrix[i, j] = np.inf
    dtw_matrix[0, 0] = 0

    for i in range(1, n + 1):
        for j in range(1, m + 1):
            cost = abs(s[i - 1] - t[j - 1])
            # take last min from a square box
            last_min = np.min([dtw_matrix[i - 1, j], dtw_matrix[i, j - 1], dtw_matrix[i - 1, j - 1]])
            dtw_matrix[i, j] = cost + last_min
    return dtw_matrix


# lesser the distance more is the similarity
def compare(req_power_arr):
    print("Window comparison between main file and target started")
    win = len(req_power_arr)
    power_window_0 = []
    power_window_1 = []
    y = np.array(req_power_arr)
    j = 0
    for line in backupfile:
        line = line.strip()
        data = line.split(DELIM)
        powerp_tot_avg_0 = data[0]
        powerp_tot_avg_1 = float(data[1])
        if j < win:
            power_window_0.append(powerp_tot_avg_0)
            power_window_1.append(powerp_tot_avg_1)
            j = j + 1
            continue

        if j == win:
            x = np.array(power_window_1)
            # fastdtw sequence
            min_distance, path = fastdtw(x, y)

            # normaldtw sequence
            # dist = dtw(x,y)
            # min_distance = dist[win][win]

        req_start = power_window_0
        req_window = power_window_1
        power_window_0.pop(0)
        power_window_1.pop(0)
        power_window_0.append(powerp_tot_avg_0)
        power_window_1.append(powerp_tot_avg_1)
        x = np.array(power_window_1)

        # fastdtw sequence O(n)
        distance, path = fastdtw(x, y)

        # normaldtw sequence O(n^2)
        # dist = dtw(x,y)
        # distance = dist[win][win]

        if distance < min_distance:
            req_start = power_window_0
            req_window = power_window_1
            min_distance = distance
        print("Current distance: " + str(distance) + " Current start time: " + str(powerp_tot_avg_0))
        j = j + 1

    return req_start, req_window, min_distance


# case1: Only Fridge
target_file = open(target_file_fridge)
print("Started: " + target_file_fridge)
lines = target_file.readlines()
lines = lines[108:]   # lines starting from 29/03/2022
req_power_array = []
for line in lines:
    arr = line.split(DELIM)
    powerp_field = arr[5]
    if powerp_field == "  NAN ":
        continue
    req_power_array.append(float(powerp_field))
fridge_start_time, fridge_window, fridge_index = compare(req_power_array)
target_file.close()
print("Ended: " + target_file_fridge)
i = 0
out_file = open(output_file_fridge, "w")
out_file.write("Fridge_time" + DELIM + "PowerP_Tot_Avg" + DELIM + "Fridge_power" + "\n")
for i in range(0, len(req_power_array)):
    out_file.write(str(fridge_start_time[i]) + DELIM + str(fridge_window[i]) + DELIM + str(req_power_array[i]) + "\n")
out_file.close()
print("Fridge's most similar start time is: " + str(fridge_start_time[0]))
print("Fridge's smallest similarity index is: " + str(fridge_index))
print("Data is in file fridge_dtw.csv ")
