import pandas as pd
import os
import threading
import time

start_time = time.time()

a_i_directory_path = r"C:\Users\Blake Dennett\Downloads\Spring2023\appliedProgramming\Data\stock_market_data\sp500\csv"
with_slash = r"C:\Users\Blake Dennett\Downloads\Spring2023\appliedProgramming\Data\stock_market_data\sp500\csv\'"

d_i_directory_path = r"C:\Users\Blake Dennett\Downloads\Spring2023\appliedProgramming\Data\stock_market_data\sp500\csv\d-i"
with_slash4 = r"C:\Users\Blake Dennett\Downloads\Spring2023\appliedProgramming\Data\stock_market_data\sp500\csv\d-i\'"

j_p_directory_path = r"C:\Users\Blake Dennett\Downloads\Spring2023\appliedProgramming\Data\stock_market_data\sp500\csv\j-p"
with_slash2 = r"C:\Users\Blake Dennett\Downloads\Spring2023\appliedProgramming\Data\stock_market_data\sp500\csv\j-p\'"

q_z_directory_path = r"C:\Users\Blake Dennett\Downloads\Spring2023\appliedProgramming\Data\stock_market_data\sp500\csv\j-p\q-z"
with_slash3 = r"C:\Users\Blake Dennett\Downloads\Spring2023\appliedProgramming\Data\stock_market_data\sp500\csv\j-p\q-z\'"

df = pd.DataFrame()
df2 = pd.DataFrame()
df3 = pd.DataFrame()
df4 = pd.DataFrame()
dataframes = []



# function to combine two datasets
def combine(dat1, dat2):
    to_combine = [dat1, dat2]
    return pd.concat(to_combine)


# function to loop through and open files
def files_to_dataframe(directory_path, with_slash, df):
    for filename in os.listdir(directory_path):
        if filename == 'j-p'or filename == 'q-z' or filename == 'd-i':
            continue
        directory = with_slash[:-1]
        path = directory + filename
        with open(path, 'r') as file:
            dat = pd.read_csv(file)
        dat['company'] = filename[:len(filename)-4]
        df = combine(df, dat)
    dataframes.append(df)

thread1 = threading.Thread(target=files_to_dataframe, args=(a_i_directory_path, with_slash, df))
thread2 = threading.Thread(target=files_to_dataframe, args=(j_p_directory_path, with_slash2, df2))
thread3 = threading.Thread(target=files_to_dataframe, args=(q_z_directory_path, with_slash3, df3))
thread4 = threading.Thread(target=files_to_dataframe, args=(d_i_directory_path, with_slash4, df4))

thread1.start()
thread2.start()
thread3.start()
thread4.start()
thread1.join()
thread2.join()
thread3.join()
thread4.join()


df = combine(dataframes[0], dataframes[1])
df2 = combine(dataframes[2], dataframes[3])
df = combine(df, df2)
print(df.head())
print(len(df))


# files_to_dataframe(a_i_directory_path, with_slash, df)
# files_to_dataframe(j_z_directory_path, with_slash2, df2)
print("--- %s seconds ---" % (time.time() - start_time))

# ============================================= add a column with the company id on it ======================