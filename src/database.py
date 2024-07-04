import mysql.connector as conn
import sys
import os
import pandas as pd
from dotenv import load_dotenv
from exception import CustomException
from logger import logging

def configure():
    load_dotenv()

configure()
host = os.getenv("database_host_name")
user = os.getenv("database_user_name")
password = os.getenv("database_user_password")
database = os.getenv("database_name")

def create_database(host, user, password):
    try:
        mydb = conn.connect(host=host, user=user, password=password)
        cursor = mydb.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS naukri")
        logging.info("Database created successfully")
    except Exception as e:
        logging.error(f"An error occurred while creating database: {e}")
        raise CustomException(f"An error occurred while creating database: {e}", sys)
    finally:
        cursor.close()
        mydb.close()

def connect_to_mysql_database(host, user, password, database):
    try:
        mydb = conn.connect(host=host, user=user, password=password, database=database)
        logging.info("Connected to MySQL successfully!")
        return mydb
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(f"An error occurred: {e}", sys)

def create_cursor_object(mydb):
    try:
        cursor = mydb.cursor()
        logging.info("Cursor object obtained successfully!")
        return cursor
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(f"An error occurred: {e}", sys)

def create_tables(host, user, password, database):
    try:
        mydb = connect_to_mysql_database(host, user, password, database)
        cursor = create_cursor_object(mydb)
        table_queries = [
            """
            CREATE TABLE IF NOT EXISTS naukri.job_data(
                ID INT AUTO_INCREMENT PRIMARY KEY,
                DATE DATE,
                TIME TIME,
                job_title VARCHAR(255),
                company_name VARCHAR(255),
                location VARCHAR(255)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS naukri.job_filtered_data(
                ID INT AUTO_INCREMENT PRIMARY KEY,
                DATE DATE,
                TIME TIME,
                job_title VARCHAR(255),
                company_name VARCHAR(255),
                location VARCHAR(255)
            )
            """
        ]

        for query in table_queries:
            cursor.execute(query)

        logging.info("Tables and columns created successfully")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(f"An error occurred: {e}", sys)
    finally:
        cursor.close()
        mydb.close()

def alter_table_add_column(host, user, password, database, table_name, column_name="job_url", column_type="VARCHAR(255)"):
    try:
        mydb = connect_to_mysql_database(host, user, password, database)
        cursor = create_cursor_object(mydb)
        alter_query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
        cursor.execute(alter_query)
        logging.info(f"Column {column_name} added to {table_name} successfully")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise CustomException(f"An error occurred: {e}", sys)
    finally:
        cursor.close()
        mydb.close()

create_database(host=host, user=user, password=password)

mydb = connect_to_mysql_database(host, user, password, database)
cursor = create_cursor_object(mydb)

create_tables(host, user, password, database)

# alter_table_add_column(host, user, password, database, "naukri.job_data", "job_url", "VARCHAR(255)")
# alter_table_add_column(host, user, password, database, "naukri.job_filtered_data", "job_url", "VARCHAR(255)")


















# def save_data(host, user, password, database):
#     try:
#         mydb = connect_to_mysql_database(host, user, password, database)
#         cursor = create_cursor_object(mydb)
 
#         folder_path = 'Bd_work_csv'
 
#         for filename in os.listdir(folder_path):
#             if filename.endswith('.csv'):
#                 csv_file_path = os.path.join(folder_path, filename)
#                 df = pd.read_csv(csv_file_path)
 
#                 for index, row in df.iterrows():
#                     role = row['ROLE'].replace("'", "''")
#                     company_name = row['COMPANY_NAME'].replace("'", "''")
#                     location = row['LOCATION'].replace("'", "''")
#                     link = row['LINK'][:255] if len(row['LINK']) > 255 else row['LINK']  # Ensure link length is within limit
 
#                     query = f"INSERT INTO linkedin.job_data (ROLE, COMPANY_NAME, LOCATION, LINK) VALUES ('{role}', '{company_name}', '{location}', '{link}')"
#                     cursor.execute(query)
       
#         mydb.commit()  # Commit changes after all inserts
#         logging.info("Data imported successfully!")
 
#     except Exception as e:
#         logging.error(f"An error occurred: {e}")
#         raise CustomException(e, sys)

# def save_data(host, user, password, database, folder_path):
#     try:
#         mydb = connect_to_mysql_database(host, user, password, database)
#         cursor = create_cursor_object(mydb)

#         for filename in os.listdir(folder_path):
#             if filename.endswith('.csv'):
#                 csv_file_path = os.path.join(folder_path, filename)
#                 df = pd.read_csv(csv_file_path)

#                 for index, row in df.iterrows():
#                     role = row['ROLE'].replace("'", "''")
#                     company_name = row['Company_Name'].replace("'", "''")
#                     location = row['Location'].replace("'", "''")
#                     link = row['Link'][:255] if len(row['Link']) > 255 else row['Link']  # Ensure link length is within limit

#                     query = f"INSERT INTO linkedin.job_data (ROLE, COMPANY_NAME, LOCATION, LINK) VALUES ('{role}', '{company_name}', '{location}', '{link}')"
#                     cursor.execute(query)
        
#         mydb.commit()  # Commit changes after all inserts
#         logging.info("Data imported successfully!")

#     except Exception as e:
#         logging.error(f"An error occurred: {e}")
#         raise CustomException(e, sys)

# folder_path = 'Bd_work_csv'
# save_data(host, user, password, database, folder_path)

# def save_filtered_data(host, user, password, database, folder_path):
#     try:
#         mydb = connect_to_mysql_database(host, user, password, database)
#         cursor = create_cursor_object(mydb)

#         for filename in os.listdir(folder_path):
#             if filename.endswith('.csv'):
#                 csv_file_path = os.path.join(folder_path, filename)
#                 df = pd.read_csv(csv_file_path)

#                 for index, row in df.iterrows():
#                     role = row['ROLE'].replace("'", "''")
#                     company_name = row['Company_Name'].replace("'", "''")
#                     location = row['Location'].replace("'", "''")
#                     link = row['Link'][:255] if len(row['Link']) > 255 else row['Link']  # Ensure link length is within limit

#                     query = f"INSERT INTO linkedin.job_filtered_data (ROLE, COMPANY_NAME, LOCATION, LINK) VALUES ('{role}', '{company_name}', '{location}', '{link}')"
#                     cursor.execute(query)
        
#         mydb.commit()  # Commit changes after all inserts
#         logging.info("Filtered data imported successfully!")

#     except Exception as e:
#         logging.error(f"An error occurred: {e}")
#         raise CustomException(e, sys)

# folder_path = 'filtered_csv_data'
# save_filtered_data(host, user, password, database, folder_path)






