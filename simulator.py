import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

# Faker generates realistic fake data (words, IDs, etc.)
fake = Faker()

# The producer is the "hand" that places events onto Redpanda.
# bootstrap_servers tells it where Redpanda is listening.
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # convert dict -> JSON -> bytes
)

# A fake product catalog: 20 made-up products with a SKU, name, and price
PRODUCTS = [
    {"sku": f"SKU-{i}", "name": fake.word().capitalize() + " " + fake.word().capitalize(), "price": round(random.uniform(10, 300), 2)}
    for i in range(1, 21)
]

# Builds one fake "order placed" event
def make_order():
    product = random.choice(PRODUCTS)
    qty = random.randint(1, 3)
    return {
        "event_type": "order",
        "order_id": str(uuid.uuid4()),       # unique ID per order
        "customer_id": fake.uuid4(),         # fake customer
        "sku": product["sku"],
        "product_name": product["name"],
        "quantity": qty,
        "unit_price": product["price"],
        "total": round(product["price"] * qty, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

# Builds a "payment" event tied to an order (95% succeed, 5% fail — for realism)
def make_payment(order):
    return {
        "event_type": "payment",
        "order_id": order["order_id"],
        "amount": order["total"],
        "status": random.choices(["success", "failed"], weights=[95, 5])[0],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

# Builds a "return" event tied to an order (only happens sometimes, see below)
def make_return(order):
    return {
        "event_type": "return",
        "order_id": order["order_id"],
        "sku": order["sku"],
        "reason": random.choice(["defective", "wrong_size", "changed_mind"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

# Builds an "inventory" event — stock goes down when something sells
def make_inventory_update(product):
    return {
        "event_type": "inventory",
        "sku": product["sku"],
        "change": -random.randint(1, 5),   # negative = stock decreasing
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

print("Starting event simulator. Press Ctrl+C to stop.")

try:
    # Infinite loop = keeps generating events until you stop it
    while True:
        order = make_order()
        producer.send("orders", order)              # send to the "orders" topic (lane)
        print("order:", order["order_id"], order["sku"], order["total"])

        payment = make_payment(order)
        producer.send("payments", payment)           # send to "payments" topic

        producer.send("inventory", make_inventory_update({"sku": order["sku"]}))  # "inventory" topic

        if random.random() < 0.1:                    # ~10% chance of a return
            producer.send("returns", make_return(order))  # "returns" topic

        time.sleep(random.uniform(0.3, 1.2))         # pause briefly, like real traffic isn't instant

except KeyboardInterrupt:
    # Runs when you press Ctrl+C — stops cleanly instead of crashing
    print("Stopped.")
    producer.flush()   # make sure any events still "in transit" get sent before exiting