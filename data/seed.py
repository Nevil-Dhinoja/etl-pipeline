import pandas as pd
import numpy as np
from faker import Faker
import random
import os

fake = Faker('en_IN')
random.seed(42)
np.random.seed(42)
Faker.seed(42)

os.makedirs("data/raw", exist_ok=True)

# --- Sales CSV ---
products = ["Laptop","Phone","Tablet","Headphones","Camera",
            "Keyboard","Monitor","Mouse","Speaker","Charger"]
regions  = ["North","South","East","West","Central"]
statuses = ["completed","returned","cancelled","pending"]

sales = pd.DataFrame({
    "order_id":      [f"ORD-{i:05d}" for i in range(1, 1001)],
    "customer_name": [fake.name() for _ in range(1000)],
    "product":       [random.choice(products) for _ in range(1000)],
    "quantity":      [random.randint(1, 10) for _ in range(1000)],
    "unit_price":    [round(random.uniform(199, 9999), 2) for _ in range(1000)],
    "region":        [random.choice(regions) for _ in range(1000)],
    "status":        [random.choice(statuses) for _ in range(1000)],
    "order_date":    [fake.date_between(start_date="-1y", end_date="today")
                      for _ in range(1000)],
    "sales_rep":     [fake.name() for _ in range(1000)],
})

# inject anomalies intentionally for detection
sales.loc[10, "unit_price"] = 999999.99   # price spike
sales.loc[20, "quantity"]   = 500          # quantity outlier
sales.loc[30, "unit_price"] = -100         # negative price (invalid)
sales.loc[40, "customer_name"] = None      # null name
sales.loc[50, "order_id"]   = "ORD-00001" # duplicate order_id

sales.to_csv("data/raw/sales.csv", index=False)
print(f"sales.csv — {len(sales)} rows")

# --- Inventory CSV ---
inventory = pd.DataFrame({
    "product":        products,
    "stock_quantity": [random.randint(0, 500) for _ in range(10)],
    "reorder_level":  [random.randint(10, 50)  for _ in range(10)],
    "warehouse":      [random.choice(["WH-A","WH-B","WH-C"]) for _ in range(10)],
    "last_updated":   [fake.date_between(start_date="-30d", end_date="today")
                       for _ in range(10)],
    "unit_cost":      [round(random.uniform(99, 4999), 2) for _ in range(10)],
})

# inject anomaly — stock below reorder level
inventory.loc[2, "stock_quantity"] = 0
inventory.to_csv("data/raw/inventory.csv", index=False)
print(f"inventory.csv — {len(inventory)} rows")

# --- Customers CSV (from fake API simulation) ---
customers = pd.DataFrame({
    "customer_id":   [f"CUST-{i:04d}" for i in range(1, 201)],
    "name":          [fake.name() for _ in range(200)],
    "email":         [fake.email() for _ in range(200)],
    "city":          [random.choice(["Surat","Mumbai","Ahmedabad",
                      "Baroda","Rajkot","Pune","Delhi","Bangalore"])
                      for _ in range(200)],
    "age":           [random.randint(18, 65) for _ in range(200)],
    "customer_tier": [random.choice(["Bronze","Silver","Gold","Platinum"])
                      for _ in range(200)],
    "join_date":     [fake.date_between(start_date="-3y", end_date="today")
                      for _ in range(200)],
    "total_spent":   [round(random.uniform(0, 500000), 2) for _ in range(200)],
})

# inject anomaly — impossible age
customers.loc[5, "age"] = 200
customers.to_csv("data/raw/customers.csv", index=False)
print(f"customers.csv — {len(customers)} rows")

print("\nAll raw CSVs generated in data/raw/")
print("Anomalies injected:")
print("  sales.csv     — price spike, qty outlier, negative price, null name, duplicate id")
print("  inventory.csv — stock at zero below reorder level")
print("  customers.csv — impossible age (200)")