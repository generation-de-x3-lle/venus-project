import pymysql
import os
from dotenv import load_dotenv
from prettytable import PrettyTable

# Load environment variables from .env file
load_dotenv()
host = os.environ.get("mysql_host")
user = os.environ.get("mysql_user")
password = os.environ.get("mysql_pass")
database = os.environ.get("mysql_db")

delivery_table = os.environ.get("delivery_table")
get_delivery_mode_from = os.environ.get("get_delivery_mode")

# Establish a database connection
connection = pymysql.connect(
    host = host,
    user = user,
    password = password,
    database = database    
)

# A cursor is an object that represents a DB cursor,
# which is used to manage the context of a fetch operation.
#cursor = connection.cursor()

#############################-- CREATE CRUD FUNCTIONS --##############################

#Add record to database
def add_record_to_db(sql_statement,data,item):      
    # Execute SQL query
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_statement,data)
    except Exception as e:         
        print('\n*******************************************')
        print('------------ FAILED TO ADD RECORD: ===>', e)
        print('*******************************************\n')
    else:
        print('\n*******************************************')
        print(f'* {item.upper()} HAS BEEN ADDED SUCCESSFULLY *')
        print('*******************************************\n')   
    connection.commit()

#Add product_on_order record to database
def add_product_on_order_record_to_db(sql_statement,data,item):      
    # Execute SQL query
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_statement,data)
    except Exception as e:         
        print('\n*******************************************')
        print('------------ FAILED TO ADD RECORD: ===>', e)
        print('*******************************************\n')
    else:
        # print('\n*******************************************')
        # print(f'* {item.upper()} HAS BEEN ADDED SUCCESSFULLY *')
        # print('*******************************************\n')   
        pass
    connection.commit()   

#Add orders record to database
def add_order_record_to_db(sql_statement,data,item):      
    # Execute SQL query
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_statement,data)            
    except Exception as e:         
        print('\n*******************************************')
        print('------------ FAILED TO ADD RECORD: ===>', e)
        print('*******************************************\n')
    else:
        print('\n*******************************************')
        print(f"* {item.upper()}'S ORDER HAS BEEN ADDED SUCCESSFULLY *")
        print('*******************************************\n')           
    connection.commit()
    id = cursor.lastrowid
    return id
    

#Read record from database
def read_record_from_db(sql_statement,heading,menu):
    record_table = PrettyTable(heading)
    # Execute SQL query
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_statement)
    except Exception as e:         
        print('\n*******************************************')
        print('------------ FAILED FETCH RECORDS: ===>', e)
        print('*******************************************\n')
    else:
        # Gets all records from the database
        fetched_rows = cursor.fetchall()

        # iterate through the fetched records(rows) and display them
        for fetch_row in fetched_rows:
            record_table.add_row(fetch_row)

        print('\n*******************************************')
        print(f'* ------SUCCESSFULLY VIEWING {menu}------ *')
        print('*******************************************\n')
        print(record_table)

#Read order records from database
def read_order_records_from_db(sql_statement,heading,menu):
    record_table = PrettyTable(heading)
    # Execute SQL query
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_statement)
    except Exception as e:         
        print('\n*******************************************')
        print('------------ FAILED FETCH RECORDS: ===>', e)
        print('*******************************************\n')
    else:
        # Gets all records from the database
        fetched_rows = cursor.fetchall()

        # iterate through the fetched records(rows) and display them        
        for fetch_row in fetched_rows:
            record_table.add_row(fetch_row)
            #print(fetch_row)


        print('\n*******************************************')
        print(f'* ------SUCCESSFULLY VIEWING {menu}------ *')
        print('*******************************************\n')
        print(record_table)                               


def get_delivery_mode_from_db(sql_statement,data):
    # Execute SQL query
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_statement,data)
    except Exception as e:         
        print('\n*******************************************')
        print('------------ FAILED FETCH RECORDS: ===>', e)
        print('*******************************************\n')
    else:
        # Gets all records from the database
        fetched_rows = cursor.fetchall()
        # iterate through the fetched records(rows) and display them
        for fetch_row in fetched_rows:
            return ''.join(fetch_row)

def get_courier_mode(delivery_id):
    sql = f'{get_delivery_mode_from} {delivery_table} WHERE delivery_id = %s' 
    inserts = (delivery_id,)
    return get_delivery_mode_from_db(sql,inserts)


#Read courier records from database
def read_courier_records_from_db(sql_statement,heading,menu):
    record_table = PrettyTable(heading)
    # Execute SQL query
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_statement)
    except Exception as e:         
        print('\n********************************************')
        print('------------ FAILED FETCH RECORDS: ===>', e)
        print('*******************************************\n')
    else:
        # Gets all records from the database
        fetched_rows = cursor.fetchall()

        # iterate through the fetched records(rows) and display them
        for fetch_row in fetched_rows:
            record_table.add_row([fetch_row[0],fetch_row[1],get_courier_mode(fetch_row[2]),fetch_row[3]])

        print('\n*******************************************')
        print(f'* ---SUCCESSFULLY VIEWING {menu}--- *')
        print('*******************************************\n')
        print(record_table) 

#Update record in database
def update_record_in_db(sql_statement,data,item):
    # Execute SQL query
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_statement,data)
    except Exception as e:         
        print('\n*******************************************')
        print('------------ FAILED TO UPDATE RECORD: ===>', e)
        print('*******************************************\n')
    else:
        print('\n*******************************************')
        print(f"* {item.upper()} UPDATE IS SUCCESSFULL *")
        print('*******************************************\n')   
        connection.commit()

#Delete record from database
def delete_record_from_db(sql_statement,data):
    # Execute SQL query
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_statement,data)
    except Exception as e:         
        print('\n*******************************************')
        print('------------ FAILED TO DELETE RECORD: ===>', e)
        print('*******************************************\n')
    else:
        print('\n*******************************************')
        print('* RECORD HAS BEEN DELETED SUCCESSFULL *')
        print('*******************************************\n')   
        connection.commit()

def read_specific_record(sql_statement,data):
    # Execute SQL query
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_statement,data)
    except Exception as e:         
        print('\n*******************************************')
        print('------------ FAILED FETCH RECORDS: ===>', e)
        print('*******************************************\n')
    else:
        # Gets all records from the database
        fetched_rows = cursor.fetchall()
        # iterate through the fetched records(rows) and display them
        for fetch_row in fetched_rows:
            #return ''.join(fetch_row)
            return fetch_row[0]
    