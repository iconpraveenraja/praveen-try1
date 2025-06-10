from pymongo import MongoClient
import pandas as pd


df = pd.read_csv("D:/car_prices.csv")


client = MongoClient("mongodb://localhost:27017/")


db = client["AmazonDB"]
collection = db["Products"]


collection.insert_many(df.to_dict(orient="records"))

print("Data inserted successfully!")
