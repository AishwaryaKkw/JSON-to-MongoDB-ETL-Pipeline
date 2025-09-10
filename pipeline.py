import json
from pathlib import Path
from pymongo import MongoClient

# Extract
json_file = Path("sample_data.json")

with open(json_file, "r") as f:
    data = json.load(f)

print("Extracted JSON data - Done")

# Transform
for order in data:
    # Calculate total for each order
    total = sum(item["qty"] * item["price"] for item in order["items"])
    order["total"] = total

print("Transformed data - Done")

# Load
#connect to local MongoDB
client = MongoClient("mongodb+srv://aishwaryakankanwadi_db_user:lYKjKHAuI0sr4Atf@jsontomogocluster0.afpajgo.mongodb.net/?retryWrites=true&w=majority&appName=JsonToMogoCluster0") 
db = client["etl_demo"]
orders_collection = db["orders"]

# Insert data into MongoDB
orders_collection.delete_many({})  # clear old records
orders_collection.insert_many(data)

print("Loading data into MongoDB- Done")


# Query 1
print("\n Orders by Customer (Aggregation):")
pipeline = [
    {"$group": {"_id": "$customer", "total_spent": {"$sum": "$total"}}},
    {"$sort": {"total_spent": -1}}
] #query 1

print("\n Top Customers by Spending :")
pipeline = [
  { "$group": { "_id": "$customer", "total_spent": { "$sum": "$total" } } },
  { "$sort": { "total_spent": -1 } },
  { "$limit": 3 }
] #query2 -Top Customers by Spending

print("\n Most Frequently Bought Products:")
pipeline = [
  { "$unwind": "$items" },
  { "$group": { "_id": "$items.product", "total_qty": { "$sum": "$items.qty" } } },
  { "$sort": { "total_qty": -1 } },
  { "$limit": 5 }
] #query3 - Most Frequently Bought Products

print("\n Average Order Value per Customer:")
pipeline = [
  { "$group": { "_id": "$customer", "avg_order_value": { "$avg": "$total" } } },
  { "$sort": { "avg_order_value": -1 } }
] #query4 - Average Order Value per Customer

print("\n Total revenue:")
pipeline = [
  { "$group": { "_id": None, "total_revenue": { "$sum": "$total" } } }
] #qwuery5 - Total revenue

for result in orders_collection.aggregate(pipeline):
    print(result)


#  Save Aggregations

# Customer Summary
customer_summary = list(orders_collection.aggregate([
    {"$group": {"_id": "$customer", "total_spent": {"$sum": "$total"}}},
    {"$sort": {"total_spent": -1}}
]))
db["customer_summary"].delete_many({})
db["customer_summary"].insert_many(customer_summary)

# Product Summary
product_summary = list(orders_collection.aggregate([
    {"$unwind": "$items"},
    {"$group": {"_id": "$items.product", "total_qty": {"$sum": "$items.qty"}}},
    {"$sort": {"total_qty": -1}}
]))
db["product_summary"].delete_many({})
db["product_summary"].insert_many(product_summary)

# Revenue Summary (grand total)
revenue_summary = list(orders_collection.aggregate([
    {"$group": {"_id": None, "total_revenue": {"$sum": "$total"}}}
]))
db["revenue_summary"].delete_many({})
db["revenue_summary"].insert_many(revenue_summary)

print("✅ Aggregation results saved into MongoDB")