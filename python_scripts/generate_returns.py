import pandas as pd
import random
import os

# Define relative paths to data folder
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "..", "data")

orders_path = os.path.join(DATA_DIR, "olist_orders_dataset.csv")
items_path = os.path.join(DATA_DIR, "olist_order_items_dataset.csv")
reviews_path = os.path.join(DATA_DIR, "olist_order_reviews_dataset.csv")

# Load datasets
orders = pd.read_csv(orders_path)
items = pd.read_csv(items_path)
reviews = pd.read_csv(reviews_path)

# Merge: orders -> items -> reviews
merged = orders.merge(items, on="order_id", how="inner").merge(reviews, on="order_id", how="inner")

# Filter only low review scores (1 or 2) as proxies for returns
returns = merged[merged['review_score'] <= 2].copy()

# Simulate return reasons
reasons = ['Defective', 'Wrong item', 'Not as described', 'Late delivery', 'Changed mind']
returns['return_reason'] = [random.choice(reasons) for _ in range(len(returns))]

# Estimate refund as product price + freight
returns['refund_amount'] = returns['price'] + returns['freight_value']

# Simplify the final dataset
return_data = returns[[
    'order_id',
    'product_id',
    'customer_id',
    'order_purchase_timestamp',
    'return_reason',
    'refund_amount',
    'review_comment_title',
    'review_comment_message'
]].rename(columns={
    'order_purchase_timestamp': 'return_date'
})

# Save to output CSV
output_path = os.path.join(DATA_DIR, "returns_sample.csv")
return_data.to_csv(output_path, index=False)

print(f"✅ Generated {len(return_data)} simulated return records at: {output_path}")
