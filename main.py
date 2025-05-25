import pandas as pd
from dotenv import load_dotenv
from os import getenv

load_dotenv()

CSV_RS = getenv("CSV_RS")
CSV = f"{CSV_RS}/online_retail_listing.csv"

df = pd.read_csv(CSV, sep=";", encoding="cp1251")
# print(df.isnull().sum())

print(df[ df["Customer ID"].isna() ])