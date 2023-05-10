import pandas as pd
import os


# function to combine two datasets
with open(r"C:\Users\Blake Dennett\Downloads\Spring2023\appliedProgramming\Data\stock_market_data\sp500\csv\A.csv", 'r') as a_file:
    dat = pd.read_csv(a_file)
    with open(r"C:\Users\Blake Dennett\Downloads\Spring2023\appliedProgramming\Data\stock_market_data\sp500\csv\AAL.csv", 'r') as all_file:
        dat2 = pd.read_csv(all_file)


def combine(dat1, dat2):
    to_combine = [dat1, dat2]
    return pd.concat(to_combine)


df = pd.DataFrame()

dat3 = combine(dat, df)

# print(len(dat))
# print(len(dat2))
# print(len(dat3))
# print(dat3.head())







# function to loop through and open files
df = pd.DataFrame()


for filename in os.listdir(r"C:\Users\Blake Dennett\Downloads\Spring2023\appliedProgramming\Data\stock_market_data\sp500\csv"):
    directory = r"C:\Users\Blake Dennett\Downloads\Spring2023\appliedProgramming\Data\stock_market_data\sp500\csv\'"
    directory = directory[:-1]
    path = directory + filename
    with open(path, 'r') as file:
        dat = pd.read_csv(file)
    df = combine(df, dat)


print(df.head())
print(len(df))


# ============================================= add a column with the company name on it ======================