from pymongo import MongoClient
import pandas as pd

# Load CSV
df = pd.read_csv("D:/car_prices.csv")

# Connect to local MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Create or connect to a database and collection
db = client["AmazonDB"]
collection = db["Products"]

# Convert DataFrame to dictionary and insert
collection.insert_many(df.to_dict(orient="records"))

print("Data inserted successfully!")
