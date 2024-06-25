from fastapi import FastAPI
import mysql.connector
from mysql.connector import Error
import time
import json

app = FastAPI()

# Function to load configuration
def load_config():
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    return config

config = load_config()




# Function to create a connection to the MySQL database
def create_connection():
    # Establish a connection to the database
    connection = mysql.connector.connect(
        host=config["DB_HOST"],
        user=config["DB_USER"],
        password=config["DB_PASS"],
        database=config["DB_NAME"]
    )
    # Print a success message if the connection is established
    print("connection created successfully")
    # Return the connection object
    return connection

def select_execute_query(connection, query):
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(query)
        data = cursor.fetchall()
        print("Data Fetched Successfully")
        return data
    except Error as e:
        print(f"The error in SELECT query '{e}' occurred")
        return None
    
def insert_query(connection, query, value):
    # Create a cursor object to execute SQL queries
    cursor = connection.cursor()
    try:
        # Execute the SQL query with the given value
        cursor.execute(query, value)
        # Commit the changes to the database
        connection.commit()
        # Return the number of rows inserted
        return cursor.rowcount
    except Error as e:
        # Print an error message if an exception occurs
        print(f"Error: {e}")
        # Return 0 if an error occurs
        return 0



connection = create_connection()

# Fetch all details of customers data with default limit=100
@app.get("/")
def fetch_all_customers(limit: int = 100):
    query = f"SELECT * FROM customers LIMIT {limit}"
    data = select_execute_query(connection, query)
    return data

#insert data in the customers data
@app.post("/insert_data/")
def insert_data(id: int=0, amount: int=0, mobileno: int=0, pincode: int=0):
    current_datetime = time.strftime('%Y-%m-%d %H:%M:%S')
    query = "INSERT INTO customers (customer_id, transaction_amount, mobile_no, transaction_datetime, pincode) VALUES (%s, %s, %s, %s, %s)"
    values = (id, amount, mobileno, current_datetime, pincode)
    insert_query(connection, query, values)
    return {"message": "Data inserted successfully"}


#fetch the result with range of transaction amount

@app.get("/transaction_range/")

def transaction_range(start: int=0, end: int=0, limit: int=100):
    query = f"SELECT * FROM customers WHERE transaction_amount BETWEEN {start} AND {end} LIMIT {limit}"
    data = select_execute_query(connection, query)
    return data


#fetch the top5 customers for each pincode with input state
@app.get("/top_customers_per_pincode/")
def top_customers_per_pincode(state: str=''):
    query = """Select distinct  c.Customer_id, c.Mobile_no, c.Pincode 
            from ( Select c.Customer_id, c.Mobile_no, c.transaction_amount, c.Pincode, @pincode_rank := IF(@current_pincode =  c.Pincode, @pincode_rank + 1, 1) AS pincode_rank, 
            @current_pincode:=  c.Pincode FROM customers as c ORDER BY  c.Pincode,  c.transaction_amount desc ) c 
            LEFT JOIN  state_master as p ON c.pincode = p.pincode where pincode_rank<=5 and p.statename = %s"""
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query, (state,))
    data = cursor.fetchall()
    return data