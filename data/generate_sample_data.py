"""
Generate realistic e-commerce event data to simulate Kinesis stream input.
Produces 1M+ events with proper distribution for testing pipelines.
"""
import json
import random
import uuid
from datetime import datetime, timedelta

PRODUCTS = [
    {"id": "P1001", "name": "Wireless Headphones", "category": "Electronics", "brand": "Sony", "price": 129.99},
    {"id": "P1002", "name": "Running Shoes", "category": "Sports", "brand": "Nike", "price": 89.99},
    {"id": "P1003", "name": "Coffee Maker", "category": "Home", "brand": "Keurig", "price": 79.99},
    {"id": "P1004", "name": "Laptop Stand", "category": "Electronics", "brand": "TwelveSouth", "price": 49.99},
    {"id": "P1005", "name": "Yoga Mat", "category": "Sports", "brand": "Lululemon", "price": 68.00},
    {"id": "P1006", "name": "Blender", "category": "Home", "brand": "Vitamix", "price": 249.99},
    {"id": "P1007", "name": "Smart Watch", "category": "Electronics", "brand": "Apple", "price": 399.99},
    {"id": "P1008", "name": "Desk Lamp", "category": "Home", "brand": "Philips", "price": 45.00},
]

EVENT_TYPES = ['view', 'click', 'cart_add', 'purchase']
DEVICES = ['mobile', 'desktop', 'tablet']
USERS = [f'U{str(i).zfill(6)}' for i in range(1, 10001)]  # 10K users

def generate_event(timestamp):
    user = random.choice(USERS)
    product = random.choice(PRODUCTS)
    event_type = random.choices(EVENT_TYPES, weights=[50, 25, 15, 10])[0]
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user,
        "session_id": str(uuid.uuid4())[:16],
        "product_id": product["id"],
        "event_type": event_type,
        "price": product["price"] if event_type == "purchase" else None,
        "timestamp": int(timestamp.timestamp() * 1000),
        "category": product["category"],
        "brand": product["brand"],
        "device_type": random.choice(DEVICES),
        "ip_address": f"192.168.{random.randint(1,255)}.{random.randint(1,255)}"
    }

def main():
    events = []
    base_time = datetime.now() - timedelta(days=1)
    for i in range(1000000):  # 1M events
        ts = base_time + timedelta(seconds=random.randint(0, 86400))
        events.append(generate_event(ts))
        if len(events) >= 10000:
            with open(f'data/raw_events_{i//10000}.jsonl', 'w') as f:
                for e in events:
                    f.write(json.dumps(e) + '\n')
            events = []
    print("Generated 1M sample events")

if __name__ == '__main__':
    main()
