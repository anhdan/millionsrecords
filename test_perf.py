import pymongo
import time
import datetime
import pandas as pd


client = pymongo.MongoClient("mongodb://root:6VPFv3vTy2x6@3.38.31.225:27017")
db = client["MillionsRecords"]
collection = db["7Mcompanies"]

def profiling_search( query: str ):

    # start timer
    start = datetime.datetime.now()

    # search
    result = collection.find_one( { "name": {"$regex" : query} } )

    # end timer
    end = datetime.datetime.now()
    duration = (end - start).total_seconds() * 1000

    return duration


if __name__ == "__main__":
    
    # read csv file
    df = pd.read_csv("companies_sorted.csv", encoding="utf-8")

    # take random sample of 10000 rows
    df = df.sample(n=10000)

    # iterate through rows
    total_time = 0
    for i, row in df.iterrows():
        # get name
        name = row["name"]

        # search
        duration = profiling_search( name )

        # add duration to total
        total_time += duration

        # print progress
        if(i % 10 == 0):
            print(f"Profiled {i} records / 10000")

    # print average
    print( "\n-----------------------------------")
    print(f"Average search time: {total_time / 10000} ms")