import sqlite3

conn = sqlite3.connect("./data/ecommerce.db")
print("Injecting bad data...")

# 1. Invalid order status
conn.execute("INSERT INTO orders VALUES (9991, 1, 1, 50.0, 'UNKNOWN_STATUS', '2023-01-01')")

# 2. Negative price in products  
conn.execute("INSERT INTO products VALUES (999, 'Bad Product', 'Electronics', -99.99, 10)")

# 3. Invalid country code in users
conn.execute("INSERT INTO users VALUES (9999, 'foreign@test.com', 'MARS', 30, '2023-01-01')")

# 4. Invalid email format
conn.execute("INSERT INTO users VALUES (8888, 'not-an-email', 'US', 22, '2023-01-01')")

# 5. Massive order total (outlier)
conn.execute("INSERT INTO orders VALUES (9992, 1, 1, 999999.99, 'paid', '2023-01-01')")

conn.commit()
conn.close()
print("Bad data injected. Now run: python run_gx_suite.py")
