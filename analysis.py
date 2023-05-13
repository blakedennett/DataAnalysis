import pandas as pd
import os
import threading
import time
import numpy as np
import yfinance as yf

start_time = round(time.time(), 2)

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


def get_company_name(id):
    msft = yf.Ticker(id)

    company_name = msft.info['longName']
    return company_name


# function to loop through and open files
def files_to_dataframe(directory_path, with_slash, df):
    for filename in os.listdir(directory_path):
        if filename == 'j-p'or filename == 'q-z' or filename == 'd-i':
            continue
        directory = with_slash[:-1]
        path = directory + filename
        with open(path, 'r') as file:
            dat = pd.read_csv(file)
        dat['company_id'] = filename[:len(filename)-4]
        df = combine(df, dat)
    dataframes.append(df)

# loops through the folder broken up into four threads that run concurently 
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



# combining the dataframes from each of the four threads
df = combine(dataframes[0], dataframes[1])
df2 = combine(dataframes[2], dataframes[3])
df = combine(df, df2)
print(len(df))

# df['company'] = df.apply(get_company_name, axis=1)
# df.drop(columns=['company_id'])

upload_time = round(time.time(), 2)
print(f"--- %s seconds ---" % (upload_time - start_time))



# ==================================================== DATA ANALYSIS =========================================


# what is the biggest difference in the low and high?

df['difference'] = df.apply(lambda x: x.High - x.Low, axis=1)

max = df["difference"].max()
df.set_index("difference", inplace=True)

high = df.loc[max,'High']
low = df.loc[max,'Low']
company = get_company_name(df.loc[max,'company_id'])
date = df.loc[max, 'Date']

print(f"The biggest difference in high to low is {max} from {company}")
print(f'The high was {high} and the low was {low}')
print(f'The date was {date}')

difference_time = round(time.time(), 2)
print("--- %s seconds ---" % (difference_time - upload_time))
df.reset_index(inplace=True)


# What is the largest difference by percentage?

df['percent_difference'] = df.apply(lambda x: (x.difference / x.High) * 100, axis=1)
max = df['percent_difference'].max()
print(df.head())
df.set_index("percent_difference", inplace=True)
company = df.loc[max, 'company_id']
high = df.loc[max, 'High']
low = df.loc[max, 'Low']
date = df.loc[max, 'Date']

print(f"The biggest difference by percent from high to low is {max} from {get_company_name(company)}")
print(f'The high was {high} and the low was {low}')
print(f'The date was {date}')

df.reset_index(inplace=True)

percentage_time = time.time()
print("--- %s seconds ---" % (percentage_time - difference_time))
