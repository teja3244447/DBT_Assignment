import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import os
import pandas as pd
import re

load_dotenv()

conn=snowflake.connector.connect(
    user=os.getenv("USER"),
    password=os.getenv("PASSWORD"),
    account=os.getenv("ACCOUNT"),
    warehouse=os.getenv("WAREHOUSE"),
    database=os.getenv("DATABASE"),
    schema=os.getenv("SCHEMA")
)
cursor = conn.cursor()
cursor.execute("CREATE SCHEMA IF NOT EXISTS RAW")
cursor.execute("CREATE SCHEMA IF NOT EXISTS FINAL")
cursor.close()


df_csv=pd.read_csv("sample_users_20.csv")
df_xlsx=pd.read_excel("sample_users.xlsx")



## RAW LAYER ###
df = pd.concat([df_csv, df_xlsx], ignore_index=True)


df["gender"]=(df["gender"]
              .str.strip()
              .str.lower()
              .replace({"male":"M","female":"F","other":"O","m":"M","f":"F"})
              )


def parse_dob(x):
    x = str(x).strip().replace('"', '')

    # YYYY/MM/DD
    if re.fullmatch(r"\d{4}/\d{2}/\d{2}", x):
        return pd.to_datetime(x, format="%Y/%m/%d")

    # 8-digit dates
    if re.fullmatch(r"\d{8}", x):
        year_first = int(x[:4])
        if 1800 <= year_first <= 2099:
            return pd.to_datetime(x, format="%Y%m%d", errors="coerce")
        else:
            return pd.to_datetime(x, format="%d%m%Y", errors="coerce")

    # Other formats (DD-MM-YYYY, Month DD, YYYY)
    return pd.to_datetime(x,dayfirst=True)

df["dob"] = df["dob"].apply(parse_dob).dt.strftime("%d-%m-%Y")
df["LOAD_TIMESTAMP"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")


success=write_pandas(
    conn,
    df,
    table_name="RAW_USER_DATA",
    schema="RAW",
    auto_create_table=True,
    overwrite=True
)
if success:
    print(f"Successfully loaded  RAW DATA into the table.")
else:
    print("Data loading failed.")



############## FINAL LAYER  ##################################

##JOIN##

merged_inner = (
    pd.merge(df_xlsx, df_csv, how="inner", on="user_id")
      .rename(columns=lambda c: c.replace("_x", "").replace("_y", ""))
)


##GENDER NORMALIZATION ##

merged_inner["gender"]=(merged_inner["gender"]
              .str.strip()
              .str.lower()
              .replace({"male":"M","female":"F","other":"O","m":"M","f":"F"})
              )


## DOB normalization ###
merged_inner["dob"]=merged_inner["dob"].apply(parse_dob).dt.strftime("%d-%m-%Y")


##current execution time ###
merged_inner["LOAD_TIMESTAMP"]=pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")



# Age calculation
today = pd.Timestamp.today().normalize()
merged_inner["age"] = (today - merged_inner["dob"]).dt.days // 365

# Age filter
merged_inner = merged_inner[merged_inner["age"] > 18]

merged_inner = merged_inner.reset_index(drop=True)

success=write_pandas(
    conn,
    merged_inner,
    table_name="FINAL_USER_DATA",
    schema="FINAL",
    auto_create_table=True,
    overwrite=True
)
if success:
    print(f"Successfully loaded  FINAL DATA into the table.")
else:
    print("Data loading failed.")

