import os
import psycopg2
from dotenv import load_dotenv,find_dotenv

# Load environment variables from .env file
load_dotenv(find_dotenv())
user = os.getenv("postgres_user")
password = os.getenv("postgres_password")
database = os.getenv("postgres_db")
host = os.getenv("postgres_host")


connection = psycopg2.connect(
    user = user, 
    password = password, 
    database= database, 
    host = host, 
)

# cursor = conn.cursor()

def create_table(sql_statement,table_name):
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_statement)
    except Exception as e: 
        print('\n*******************************************')
        print('------------ FAILED TO CREATE TABLE: ===>', e)
        print('*******************************************\n')
    else:
        print('\n*****************************************************')
        print(f'* {table_name.upper()} HAS BEEN CREATED SUCCESSFULLY *')
        print('*****************************************************\n') 
    connection.commit()  

def load_data(sql_statement):
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_statement)
    except Exception as e: 
        print('\n*******************************************')
        print('------------ FAILED TO LOAD TO TABLE(S): ===>', e)
        print('*******************************************\n')    
    connection.commit()