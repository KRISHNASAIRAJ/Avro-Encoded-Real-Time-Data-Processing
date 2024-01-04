import mysql.connector#Mysql connector
import random#Random generator
import time

def generate_product_name():#It generates product name for sql data
    products = ['T-shirt', 'Laptop', 'Shoes', 'Headphones', 'Watch']
    return random.choice(products)

def generate_category():#It generates category for sql data
    categories = ['Clothing', 'Electronics', 'Footwear', 'Accessories']
    return random.choice(categories)

def generate_price():#It generates random price
    return round(random.uniform(10, 1000), 2)

# Establish connection to MySQL
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root"
)
cursor = db.cursor()

#MYSQL QUERIES
# Create the database and table if they don't exist
cursor.execute("CREATE DATABASE IF NOT EXISTS buy_online;")#CREATE DATABASE
cursor.execute("USE buy_online;")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        product_id INT AUTO_INCREMENT PRIMARY KEY,
        product_name VARCHAR(255),
        category VARCHAR(50),
        price DECIMAL(10, 2),
        updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );#TABLE CREATION
""")

# Continuously generate and update products
while True:
    # Generate product information from the above functions
    product_name = generate_product_name()
    category = generate_category()
    price = generate_price()

    # Insert data into the database (product_id will auto-increment) and timestamp is current_time
    query = "INSERT INTO products (product_name, category, price, updated_timestamp) VALUES (%s, %s, %s, NOW())"
    values = (product_name, category, price)
    cursor.execute(query, values)
    db.commit()#Commit the changes

    # Get the last inserted ID (auto-incremented)
    last_id = cursor.lastrowid
    print(f"Inserted product ID: {last_id}")#Printing the last inserted record id
    time.sleep(5)  # Update every 5 seconds