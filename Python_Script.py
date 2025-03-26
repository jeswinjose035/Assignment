import pandas as pd
from pandas import json_normalize
import logging

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,  # Log messages at INFO level or higher
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('processing.log'),  # Logs to a file
        logging.StreamHandler()  # Logs to the console
    ]
)

try:
    # Step 1: Load JSON Data
    df = pd.read_json('assignment.json')
    logging.info("JSON data loaded successfully.")

except Exception as e:
    logging.error(f"Error loading JSON data: {e}")


try:
    # Step 2: Normalize the 'product' column
    df_normalized = pd.json_normalize(df['product'], sep='_')
    df_normalized.columns = ['product_' + col for col in df_normalized.columns]
    logging.info("Data normalized successfully.")

except Exception as e:
    logging.error(f"Error during normalization: {e}")


print(df_normalized)


try:
    # Step 3: Merge the normalized data with the original dataframe
    result = df.join(df_normalized)  # Merging by Index (join)If both DataFrames share the same index, you can merge them by index using the join() method (which merges DataFrames based on their index instead of columns).
    logging.info("Data merged successfully.")

except Exception as e:
    logging.error(f"Error during merging data: {e}")

#Scale the records by duplicating
for i in range(8):
    result=pd.concat([result, result], ignore_index=True)

print(result.count())



result = result.drop('product', axis = 1)

#Handling  negative quantity values
result['quantity'] = result['quantity'].where(result['quantity'] >= 0, 0)

try:
    result['total_value'] = (result['quantity'] * result['product_price']).round(2)
    logging.info("Total sales value calculated successfully.")
except Exception as e:
    logging.error(f"Error calculating total value: {e}")



try:
    result['formatted_date'] = pd.to_datetime(result['date'], errors='coerce').dt.strftime('%Y-%m-%d')
    result['formatted_date'] = result['formatted_date'].fillna(result['date'])
    result.drop('date', axis=1, inplace=True)
    logging.info("Date formatted successfully.")
except Exception as e:
    logging.error(f"Error during date formatting: {e}")


try:
    result['customer_id'] = result['customer_id'].fillna('Unknown')
    logging.info("Missing values handled successfully.")
except Exception as e:
    logging.error(f"Error handling missing values: {e}")


try:
    result['transaction_id'] = ['T00' + str(i+1) if i+1 <= 9 else
                                 'T0' + str(i+1) if i+1 <= 99 else
                                 'T' + str(i+1)
                                 for i in range(len(result))]
    #droping duplicate transaction_id
    result = result.drop_duplicates(subset='transaction_id', keep='first')
    logging.info("Transaction IDs generated and duplicates removed.")
except Exception as e:
    logging.error(f"Error generating transaction IDs: {e}")



import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine

connection = ""
cursor = ""

# Establish connection to MySQL server
try:
    connection = mysql.connector.connect(
        host='localhost',  # e.g., 'localhost' or IP address
        user='root',  # MySQL username
        password='root'  # MySQL password
    )

    if connection.is_connected():
        logging.info("Connected to MySQL server")


        # Create a cursor object to interact with MySQL
        cursor = connection.cursor()

        # 1. Create the database if it doesn't exist
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {'Assignment'}")
        logging.info(f"Database '{'Assignment'}' created or already exists.")


        # 2. Use the newly created database
        cursor.execute(f"USE {'Assignment'}")

        # 3. Create the table with a schema (Fixed syntax)
        create_table_query = """
        CREATE TABLE IF NOT EXISTS sales (
            transaction_id VARCHAR(100) NOT NULL PRIMARY KEY UNIQUE,
            customer_id VARCHAR(100) NOT NULL,
            quantity INT NOT NULL,
            formatted_date DATE NOT NULL,
            region VARCHAR(100) NOT NULL,
            product_id VARCHAR(100) NOT NULL,
            product_name VARCHAR(100) NOT NULL,
            product_category VARCHAR(100) NOT NULL,
            product_price FLOAT NOT NULL,
            total_value FLOAT NOT NULL
        );
        """
        cursor.execute(create_table_query)
        logging.info("Table 'sales' created successfully or already exists.")


        # # 4. Create indexes for 'formatted_date' and 'region'
        # create_index_query_1 = "CREATE INDEX idx_formatted_date ON sales (formatted_date(50));"
        # cursor.execute(create_index_query_1)
        # print("Index on 'formatted_date' created successfully.")
        #
        # create_index_query_2 = "CREATE INDEX idx_region ON sales (region(50));"
        # cursor.execute(create_index_query_2)
        # print("Index on 'region' created successfully.")

except Error as e:

    logging.error(f"Error while connecting to MySQL: {e}")

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()

        logging.info("MySQL connection is closed.")

# Define the chunk size (e.g., 1000 rows per batch)
chunksize = 500

try:
    # Create a connection string (replace with your actual connection details)
    connection_string = f"mysql+mysqlconnector://{'root'}:{'root'}@{'localhost'}/{'Assignment'}"

    # Create SQLAlchemy engine
    engine = create_engine(connection_string)

    # Use the to_sql method with chunksize to load data in batches
    result.to_sql('sales', con=engine, if_exists='append', index=False, chunksize=chunksize)
    logging.info("Data inserted successfully into the 'sales' table in batches.")

except Exception as e:
    logging.error(f"Error during data insertion into MySQL: {e}")