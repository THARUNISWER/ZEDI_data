DELIM = ','
root_file = "C:\\Users\\tharu\\Downloads\\main_data_dtw.csv"
file = open(root_file)
print("Started: " + root_file)
powerp_tot_avg_arr_0 = []
powerp_tot_avg_arr_1 = []
savefile = open("backup.csv", "w")
i = 0
for line in file:
    print("Processing line " + str(i) + " in main file")
    if i == 0:
        i += 1
        continue
    arr = line.split(DELIM)
    powerp_tot_avg_field = arr[60]
    start_time = arr[0]
    if powerp_tot_avg_field == "  NAN ":
        continue
    powerp_tot_avg_arr_0.append(start_time)
    powerp_tot_avg_arr_1.append(float(powerp_tot_avg_field))
    savefile.write(start_time + DELIM + powerp_tot_avg_field + "\n")

    # limiting number of lines for testing
    # if i == 1800:
    #     break
    i += 1

savefile.close()
file.close()
print("Ended: " + root_file)