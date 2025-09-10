JSON → MongoDB ETL Pipeline (Fitness Orders)
📌 Project Overview

This mini-project demonstrates a simple ETL (Extract → Transform → Load) pipeline using Python and MongoDB Atlas.
We use a sample dataset of fitness equipment orders, transform it by calculating totals, and load it into MongoDB.
Finally, we generate useful aggregated insights like customer spending, product sales, and total revenue.

⚙️ Tech Stack

Python 3

MongoDB Atlas 

pymongo library

🚀 Steps in the Pipeline
1. Extract

Read the JSON file (sample_data.json) containing customer orders.

2. Transform

For each order, calculate the total = sum of (qty × price) for all items.

Add this total field to each order.

3. Load

Connect to MongoDB Atlas.

Insert the transformed data into the etl_demo.orders collection.

4. Aggregation (Analysis)

We run aggregations and store results in separate collections:

Customer Summary → total amount spent per customer
Saved in: etl_demo.customer_summary

Product Summary → total quantity sold per product
Saved in: etl_demo.product_summary

Revenue Summary → total revenue across all orders
Saved in: etl_demo.revenue_summary