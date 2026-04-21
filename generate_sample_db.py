import sqlite3, random, datetime, os

os.makedirs("./data", exist_ok=True)
conn = sqlite3.connect("./data/ecommerce.db")
conn.executescript("""
    DROP TABLE IF EXISTS users;
    DROP TABLE IF EXISTS orders;
    DROP TABLE IF EXISTS products;
    CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL, country TEXT, age INTEGER, joined TEXT);
    CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, category TEXT, price REAL, stock INTEGER);
    CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product_id INTEGER, total REAL, status TEXT, created_at TEXT);
""")

countries = ["LK","US","UK","IN","AU","DE"]
for i in range(1,501):
    conn.execute("INSERT INTO users VALUES (?,?,?,?,?)",(i,f"user{i}@example.com",random.choice(countries),random.randint(18,65) if random.random()>0.05 else None,str(datetime.date(2022,1,1)+datetime.timedelta(days=random.randint(0,800)))))

categories=["Electronics","Clothing","Books","Home","Sports"]
pnames=["Laptop","Phone","Shirt","Novel","Lamp","Shoes","Tablet","Watch","Bag","Camera"]
for i in range(1,51):
    conn.execute("INSERT INTO products VALUES (?,?,?,?,?)",(i,f"{random.choice(pnames)} Model-{i}",random.choice(categories),round(random.uniform(5.99,1499.99),2),random.randint(0,500) if random.random()>0.1 else None))

statuses=["paid","pending","cancelled","refunded"]
for i in range(1,2001):
    conn.execute("INSERT INTO orders VALUES (?,?,?,?,?,?)",(i,random.randint(1,500),random.randint(1,50),round(random.uniform(5.99,2999.99),2) if random.random()>0.02 else None,random.choice(statuses),str(datetime.date(2023,1,1)+datetime.timedelta(days=random.randint(0,500)))))

conn.commit(); conn.close()
print("Sample DB created: ./data/ecommerce.db")
print("  users: 500 rows | products: 50 rows | orders: 2000 rows")
