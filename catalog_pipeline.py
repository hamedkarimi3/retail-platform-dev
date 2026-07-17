import random
import dlt
from faker import Faker

fake = Faker()

# Generates fake product catalog rows — the "static" data (doesn't stream, just exists)
def generate_products(n=20):
    for i in range(1, n + 1):
        yield {
            "sku": f"SKU-{i}",
            "name": fake.word().capitalize() + " " + fake.word().capitalize(),
            "category": random.choice(["Electronics", "Clothing", "Home", "Sports", "Books"]),
            "price": round(random.uniform(10, 300), 2),
        }

# Generates fake customer rows
def generate_customers(n=50):
    for _ in range(n):
        yield {
            "customer_id": fake.uuid4(),
            "name": fake.name(),
            "city": fake.city(),
            "signup_date": fake.date_this_decade().isoformat(),
        }

# dlt pipeline: defines WHERE data goes (destination) and under what project name
pipeline = dlt.pipeline(
    pipeline_name="retail_catalog",
    destination="duckdb",
    dataset_name="retail_staging",
)

# Run it: dlt handles table creation, schema, and loading automatically
load_info = pipeline.run(
    [
        dlt.resource(generate_products, name="products"),
        dlt.resource(generate_customers, name="customers"),
    ]
)

print(load_info)