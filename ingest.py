import pymongo
import pandas as pd


client = pymongo.MongoClient("mongodb://root:6VPFv3vTy2x6@3.38.31.225:27017")
db = client["MillionsRecords"]
collection = db["7Mcompanies"]


# read csv file
df = pd.read_csv("companies_sorted.csv", encoding="utf-8")

# get length of dataframe
length = len(df)

start = 1385000

while start < length:
    # get end index
    end = start + 10000
    if end > length:
        end = length

    # get dataframe slice
    df_slice = df[start:end]

    # convert slice to list of dicts
    data = df_slice.to_dict("records")

    # insert slice
    collection.insert_many(data)

    # print progress
    print(f"Inserted rows {start} / {length}")

    # update start
    start = end

# # loop through rows
# for i, row in df.iterrows():
#     if i < 322000:
#         continue
#     # convert row to dict
#     data = dict(row)

#     # insert row
#     collection.insert_one(data)

#     if(i % 100 == 0):
#         print(f"Inserted row {i} / {length}")

# close connection
client.close()

