import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os
 
fake = Faker()
random.seed(42)
np.random.seed(42)
Faker.seed(42)
 
OUTPUT_DIR = "/mnt/user-data/outputs/ecommerce_csvs"
os.makedirs(OUTPUT_DIR, exist_ok=True)
 
# ── 1. CATEGORIES ──────────────────────────────────────────────────────────────
NUM_CATEGORIES = 10
category_names = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books",
                  "Toys", "Beauty", "Food & Drinks", "Automotive", "Health"]
 
categories = pd.DataFrame({
    "category_id": range(1, NUM_CATEGORIES + 1),
    "name": category_names,
    "description": [fake.sentence(nb_words=8) for _ in range(NUM_CATEGORIES)]
})
categories.to_csv(f"{OUTPUT_DIR}/categories.csv", index=False)
print(f"categories: {len(categories)} rows")
 
# ── 2. PRODUCTS ────────────────────────────────────────────────────────────────
NUM_PRODUCTS = 80
product_ids = range(1, NUM_PRODUCTS + 1)
 
products = pd.DataFrame({
    "product_id": product_ids,
    "category_id": [random.randint(1, NUM_CATEGORIES) for _ in product_ids],
    "name": [fake.catch_phrase() for _ in product_ids],
    "price": [round(random.uniform(5.0, 500.0), 2) for _ in product_ids],
    "cost": [round(random.uniform(2.0, 250.0), 2) for _ in product_ids],
    "stock": [random.randint(0, 200) for _ in product_ids],
    "is_active": [random.choice([True, False]) for _ in product_ids]
})
# Ensure cost < price
products["cost"] = products.apply(lambda r: round(r["price"] * random.uniform(0.3, 0.7), 2), axis=1)
products.to_csv(f"{OUTPUT_DIR}/products.csv", index=False)
print(f"products: {len(products)} rows")
 
# ── 3. CUSTOMERS ───────────────────────────────────────────────────────────────
NUM_CUSTOMERS = 150
customer_ids = range(1, NUM_CUSTOMERS + 1)
channels = ["organic", "paid_search", "social_media", "email", "referral", "direct"]
 
customers = pd.DataFrame({
    "customer_id": customer_ids,
    "name": [fake.name() for _ in customer_ids],
    "email": [fake.unique.email() for _ in customer_ids],
    "country": [fake.country() for _ in customer_ids],
    "city": [fake.city() for _ in customer_ids],
    "acquisition_channel": [random.choice(channels) for _ in customer_ids],
    "registered_at": [fake.date_time_between(start_date="-3y", end_date="-1d") for _ in customer_ids]
})
customers.to_csv(f"{OUTPUT_DIR}/customers.csv", index=False)
print(f"customers: {len(customers)} rows")
 
# ── 4. ORDERS ──────────────────────────────────────────────────────────────────
NUM_ORDERS = 300
order_ids = range(1, NUM_ORDERS + 1)
order_statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]
 
def make_order_dates():
    ordered = fake.date_time_between(start_date="-2y", end_date="now")
    shipped = ordered + timedelta(days=random.randint(1, 5)) if random.random() > 0.2 else None
    delivered = (shipped + timedelta(days=random.randint(2, 10))) if shipped and random.random() > 0.3 else None
    return ordered, shipped, delivered
 
order_dates = [make_order_dates() for _ in order_ids]
 
orders = pd.DataFrame({
    "order_id": order_ids,
    "customer_id": [random.randint(1, NUM_CUSTOMERS) for _ in order_ids],
    "status": [random.choice(order_statuses) for _ in order_ids],
    "shipping_address": [fake.address().replace("\n", ", ") for _ in order_ids],
    "ordered_at": [d[0] for d in order_dates],
    "shipped_at": [d[1] for d in order_dates],
    "delivered_at": [d[2] for d in order_dates]
})
orders.to_csv(f"{OUTPUT_DIR}/orders.csv", index=False)
print(f"orders: {len(orders)} rows")
 
# ── 5. ORDER_ITEMS ─────────────────────────────────────────────────────────────
# Each order gets 1–5 items; product_id and unit_price must be consistent
order_item_rows = []
order_item_id = 1
 
for oid in order_ids:
    n_items = random.randint(1, 5)
    chosen_products = random.sample(list(product_ids), min(n_items, NUM_PRODUCTS))
    for pid in chosen_products:
        base_price = float(products.loc[products["product_id"] == pid, "price"].values[0])
        order_item_rows.append({
            "order_item_id": order_item_id,
            "order_id": oid,
            "product_id": pid,
            "quantity": random.randint(1, 10),
            "unit_price": base_price,
            "discount": round(random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20]), 2)
        })
        order_item_id += 1
 
order_items = pd.DataFrame(order_item_rows)
order_items.to_csv(f"{OUTPUT_DIR}/order_items.csv", index=False)
print(f"order_items: {len(order_items)} rows")
 
# ── 6. PAYMENTS ────────────────────────────────────────────────────────────────
# One payment per order (realistic simplification)
pay_methods = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]
pay_statuses = ["pending", "completed", "failed", "refunded"]
 
# Calculate total per order from order_items
order_totals = (
    order_items
    .assign(line_total=lambda df: df["quantity"] * df["unit_price"] * (1 - df["discount"]))
    .groupby("order_id")["line_total"]
    .sum()
    .round(2)
)
 
payments_rows = []
for idx, oid in enumerate(order_ids, start=1):
    ordered_at = orders.loc[orders["order_id"] == oid, "ordered_at"].values[0]
    paid_at = pd.Timestamp(ordered_at) + timedelta(hours=random.randint(0, 48))
    payments_rows.append({
        "payment_id": idx,
        "order_id": oid,
        "method": random.choice(pay_methods),
        "status": random.choice(pay_statuses),
        "amount": order_totals.get(oid, round(random.uniform(10, 500), 2)),
        "paid_at": paid_at
    })
 
payments = pd.DataFrame(payments_rows)
payments.to_csv(f"{OUTPUT_DIR}/payments.csv", index=False)
print(f"payments: {len(payments)} rows")
 
# ── 7. REVIEWS ─────────────────────────────────────────────────────────────────
# Only delivered orders can have reviews; not all order_items get reviewed
delivered_items = order_items[
    order_items["order_id"].isin(orders.loc[orders["status"] == "delivered", "order_id"])
]
# ~60% of delivered items get a review
review_sample = delivered_items.sample(frac=0.6, random_state=42)
 
reviews_rows = []
for rev_id, (_, row) in enumerate(review_sample.iterrows(), start=1):
    delivered_at = orders.loc[orders["order_id"] == row["order_id"], "delivered_at"].values[0]
    if pd.isna(delivered_at):
        reviewed_at = fake.date_time_between(start_date="-1y", end_date="now")
    else:
        reviewed_at = pd.Timestamp(delivered_at) + timedelta(days=random.randint(1, 30))
    reviews_rows.append({
        "review_id": rev_id,
        "order_item_id": row["order_item_id"],
        "rating": random.randint(1, 5),
        "comment": fake.sentence(nb_words=random.randint(5, 20)) if random.random() > 0.3 else None,
        "reviewed_at": reviewed_at
    })
 
reviews = pd.DataFrame(reviews_rows)
reviews.to_csv(f"{OUTPUT_DIR}/reviews.csv", index=False)
print(f"reviews: {len(reviews)} rows")
 
print("\n✅ Todos los CSV generados correctamente en:", OUTPUT_DIR)
 
# ── Integrity checks ───────────────────────────────────────────────────────────
print("\n── Verificación de integridad referencial ──")
assert products["category_id"].isin(categories["category_id"]).all(), "FK products->categories roto"
assert orders["customer_id"].isin(customers["customer_id"]).all(), "FK orders->customers roto"
assert order_items["order_id"].isin(orders["order_id"]).all(), "FK order_items->orders roto"
assert order_items["product_id"].isin(products["product_id"]).all(), "FK order_items->products roto"
assert payments["order_id"].isin(orders["order_id"]).all(), "FK payments->orders roto"
assert reviews["order_item_id"].isin(order_items["order_item_id"]).all(), "FK reviews->order_items roto"
print("✅ Todas las claves foráneas son válidas")