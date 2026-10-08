"""Generates a synthetic e-commerce dataset with intentional data quality issues.

Run: python generate_data.py
Output: data/raw/{customers,products,orders,order_items}.csv
"""
import csv
import os
import random
from datetime import datetime, timedelta

random.seed(42)

OUT_DIR = os.path.join(os.path.dirname(__file__), "data", "raw")
os.makedirs(OUT_DIR, exist_ok=True)

STATES = ["SP", "RJ", "MG", "RS", "PR", "BA", "SC", "PE", "CE", "GO"]
CATEGORIES = ["Electronics", "Home & Kitchen", "Fashion", "Books", "Sports", "Toys"]
STATUSES = ["delivered", "shipped", "processing", "canceled"]
PAYMENT_TYPES = ["credit_card", "boleto", "pix", "debit_card"]

N_CUSTOMERS = 50
N_PRODUCTS = 30
N_ORDERS = 500
N_ORDER_ITEMS = 1200


def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def generate_customers():
    rows = []
    for i in range(1, N_CUSTOMERS + 1):
        customer_id = f"C{i:04d}"
        rows.append([
            customer_id,
            f"Customer {i}",
            f"customer{i}@example.com",
            random.choice(["Sao Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba", "Salvador"]),
            random.choice(STATES),
            random_date(datetime(2022, 1, 1), datetime(2024, 12, 31)).date().isoformat(),
        ])
    write_csv(
        os.path.join(OUT_DIR, "customers.csv"),
        ["customer_id", "customer_name", "email", "city", "state", "signup_date"],
        rows,
    )
    return [r[0] for r in rows]


def generate_products():
    rows = []
    for i in range(1, N_PRODUCTS + 1):
        product_id = f"P{i:03d}"
        category = random.choice(CATEGORIES)
        # Inject inconsistent casing/whitespace on ~15% of rows to simulate dirty source data.
        if random.random() < 0.15:
            category = f" {category.lower()} "
        rows.append([
            product_id,
            f"Product {i}",
            category,
            round(random.uniform(15.0, 800.0), 2),
        ])
    write_csv(
        os.path.join(OUT_DIR, "products.csv"),
        ["product_id", "product_name", "category", "price"],
        rows,
    )
    return [r[0] for r in rows]


def generate_orders(customer_ids):
    rows = []
    for i in range(1, N_ORDERS + 1):
        order_id = f"O{i:05d}"
        # ~3% of orders reference a customer_id that doesn't exist (orphan records).
        if random.random() < 0.03:
            customer_id = f"C9{random.randint(100, 999)}"
        else:
            customer_id = random.choice(customer_ids)
        rows.append([
            order_id,
            customer_id,
            random_date(datetime(2024, 1, 1), datetime(2024, 12, 31)).date().isoformat(),
            random.choice(STATUSES),
            random.choice(PAYMENT_TYPES),
        ])
    write_csv(
        os.path.join(OUT_DIR, "orders.csv"),
        ["order_id", "customer_id", "order_date", "status", "payment_type"],
        rows,
    )
    return [r[0] for r in rows]


def generate_order_items(order_ids, product_ids):
    rows = []
    for i in range(1, N_ORDER_ITEMS + 1):
        order_item_id = f"OI{i:05d}"

        # ~2% reference a non-existent order; ~2% reference a non-existent product.
        order_id = f"O9{random.randint(1000, 9999)}" if random.random() < 0.02 else random.choice(order_ids)
        product_id = f"P9{random.randint(10, 99)}" if random.random() < 0.02 else random.choice(product_ids)

        quantity = random.randint(1, 5)
        # ~2% invalid quantity (zero or negative).
        if random.random() < 0.02:
            quantity = random.choice([0, -1])

        unit_price = round(random.uniform(15.0, 800.0), 2)
        # ~1% invalid negative price.
        if random.random() < 0.01:
            unit_price = -unit_price

        freight_value = round(random.uniform(5.0, 40.0), 2)

        rows.append([order_item_id, order_id, product_id, quantity, unit_price, freight_value])

    # Inject ~1% exact duplicate rows to simulate upstream duplication.
    n_dupes = int(len(rows) * 0.01)
    rows.extend(random.choice(rows[:len(rows)]) for _ in range(n_dupes))

    write_csv(
        os.path.join(OUT_DIR, "order_items.csv"),
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "freight_value"],
        rows,
    )


if __name__ == "__main__":
    customer_ids = generate_customers()
    product_ids = generate_products()
    order_ids = generate_orders(customer_ids)
    generate_order_items(order_ids, product_ids)
    print(f"Synthetic e-commerce dataset generated in: {OUT_DIR}")
