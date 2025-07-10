import sqlite3
from datetime import datetime, timedelta
import random

# Connect to SQLite DB
conn = sqlite3.connect('customer_data.db')
cursor = conn.cursor()

# Drop tables if they exist
cursor.execute("DROP TABLE IF EXISTS page_views")
cursor.execute("DROP TABLE IF EXISTS customers")

# Create Customers Table
cursor.execute('''
CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    email TEXT,
    first_name TEXT,
    last_name TEXT,
    plan_type TEXT,
    candidate TEXT
)
''')

# Create Page Views Table
cursor.execute('''
CREATE TABLE page_views (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    page TEXT,
    device TEXT,
    browser TEXT,
    location TEXT,
    event_time TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES customers(id)
)
''')

# Generate Customers
plan_types = ['free', 'basic', 'pro', 'enterprise']
first_names = ['Alice', 'Bob', 'Charlie', 'Dana', 'Eli', 'Fay', 'Grace', 'Henry', 'Ivy', 'Jack', 'Kira', 'Liam']
last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis', 'Garcia', 'Rodriguez', 'Wilson']
customers = []

for i in range(1, 15): 
    fname = random.choice(first_names)
    lname = random.choice(last_names)
    email = f"{fname.lower()}.{lname.lower()}@example.com"
    plan = random.choice(plan_types)
    customers.append((i, email, fname, lname, plan, 'Callum Hodges'))

cursor.executemany('''
INSERT INTO customers (id, email, first_name, last_name, plan_type, candidate)
VALUES (?, ?, ?, ?, ?, ?)
''', customers)

# Generate Page Views
pages = ['Home page', 'Pricing', 'Settings', 'Dashboard', 'Features']
devices = ['Dell Computer', 'MacBook', 'iPhone', 'Android Phone']
browsers = ['Chrome', 'Firefox', 'Safari', 'Edge']
locations = ['San Francisco, CA', 'New York, NY', 'Austin, TX', 'Seattle, WA', 'Los Angeles, CA']
page_views = []

now = datetime.now()

for i in range(1, 30): 
    user_id = random.choice(customers)[0]
    page = random.choice(pages)
    device = random.choice(devices)
    browser = random.choice(browsers)
    location = random.choice(locations)
    days_ago = random.randint(0, 14)
    event_time = now - timedelta(days=days_ago, hours=random.randint(0, 23))
    page_views.append((i, user_id, page, device, browser, location, event_time.strftime("%Y-%m-%d %H:%M:%S")))

cursor.executemany('''
INSERT INTO page_views (id, user_id, page, device, browser, location, event_time)
VALUES (?, ?, ?, ?, ?, ?, ?)
''', page_views)

conn.commit()
conn.close()

print("Database and tables created successfully with sample data.")
