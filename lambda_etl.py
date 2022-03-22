import os
import csv
from pprint import pp
from datetime import datetime
import logging
import boto3
import psycopg2
from dotenv import load_dotenv,find_dotenv

# # Load environment variables from .env file
# load_dotenv(find_dotenv())
# user = os.getenv("postgres_user")
# password = os.getenv("postgres_password")
# database = os.getenv("postgres_db")
# host = os.getenv("postgres_host")

# connection = psycopg2.connect(
#     user = user, 
#     password = password, 
#     database= database, 
#     host = host, 
# )

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
  id = 0
  try:
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    id = cursor.fetchone()[0]
  except Exception as e: 
      print('\n*******************************************')
      print('------------ FAILED TO LOAD TO TABLE(S): ===>', e)
      print('*******************************************\n')    
  connection.commit()
  cursor.close()
  return id

def extract_raw_data_from_csv(filename):
    raw_sales_data = []

    try:
        with open(f'/tmp/{filename}','r') as file:

            field_names = ['order_date_time', 'branch_location','customer_name', 'order_items', 'total_payment', 'payment_type', 'card_number']
            reader = csv.DictReader(file, field_names)
            
            raw_sales_data = []
            for row in reader:
                raw_sales_data.append(row)
    except Exception as err:
        print(f"An error occured: {str(err)}")

    print(raw_sales_data)
    return raw_sales_data

def remove_sensitive_data(raw_data):
    for item in raw_data:
        del item['customer_name']
        del item['card_number']
    return raw_data


def remove_whitespace_from_dict_values_in_list(list_of_dicts):
  clean_data = []
  for dict in list_of_dicts:
    clean_d_instance = {k: v.strip() for k, v in dict.items()}
    clean_data.append(clean_d_instance)
  return clean_data

def normalise_data(cleaned_data):
  normalised_data_list = []

  #Splitting order_items into a list of items
  for drink_order in cleaned_data:
    drink_order['order_items'] = drink_order['order_items'].split(',')
    
    #Change date-time to SQL format
    drink_order['order_date_time'] = datetime.strptime(drink_order['order_date_time'], '%d/%m/%Y %H:%M').strftime('%Y-%m-%d %H:%M')

    #Creating a list of dictionaries for order_items
    order_items_list = []
    for order_item in drink_order['order_items']:
      drink = order_item.split("-")[0]
      if order_item.count("-") == 2:
        flavour = order_item.split("-")[1]
        price = order_item.split("-")[2]
      elif order_item.count("-") == 1:
        flavour = "No Flavour"
        price = order_item.split("-")[1]

      hot_drink = {
        "product_name" : drink,
        "flavour" : flavour,
        "product_price" : price
      }

      order_items_list.append(hot_drink)

      no_whitespace_order_items_list = remove_whitespace_from_dict_values_in_list(order_items_list)  
      # pp(no_whitespace_order_items_list)
    drink_order['order_items'] = no_whitespace_order_items_list

    normalised_data_list.append(drink_order)

  return normalised_data_list

# pp(normalised_data)

def no_duplicate_products(data):
  all_products = []
  for order in data:
    if len(order['order_items']) < 1:
      all_products.append(order.get('order_items'))
    else: 
      for product in order['order_items']:
        all_products.append(product)
  
  no_duplicate_products = [dict(t) for t in {tuple(d.items()) for d in all_products}]
  pp(no_duplicate_products)
  
  return no_duplicate_products

#LOAD.PY

def load_data_into_db(product_data, order_data):
  products_with_id_list = []
  for product in product_data:
    sql_product = f'''
    INSERT INTO products(product,flavour,price)
    VALUES
    ('{product['product_name']}','{product['flavour']}','{product['product_price']}')
    RETURNING product_id
    '''
    product_id = db.load_data(sql_product)
    product['product_id'] = product_id

    products_with_id_list.append(product) 

  orders_with_id_list = []
  for order in order_data:
    sql_order = f'''
    INSERT INTO orders (order_date, branch_location, total_payment, payment_type)
    VALUES ('{order['order_date_time']}','{order['branch_location']}','{order['total_payment']}','{order['payment_type']}')
    RETURNING order_id
    '''
    order_id = db.load_data(sql_order)
    order['order_id'] = order_id
    orders_with_id_list.append(order)

    for order_item in order['order_items']:
      for item_with_id in products_with_id_list:
        if order_item['product_name'] == item_with_id['product_name'] and order_item['flavour'] == item_with_id['flavour']:
          product_id = item_with_id['product_id']

          sql_prods_on_order = f'''
          INSERT INTO products_on_order (order_id, product_id)
          VALUES ('{order_id}', '{product_id}')
          '''
          db.load_data(sql_prods_on_order)
  
  # pp(products_with_id_list)
  return orders_with_id_list
  
def handler(event, context):
  LOGGER = logging.getLogger()
  LOGGER.setLevel(logging.INFO)
  LOGGER.info(f'Event structure: {event}')

  bucket = 'de-x3-lle-venus'
  filename = 'chesterfield_16-03-2022_09-00-00.csv'

  s3 = boto3.resource('s3')
  s3.meta.client.download_file(bucket, f'/2022/3/16/{filename}', f'/tmp/{filename}')
  os.chdir('/tmp')
  
  raw_sales_data = extract_raw_data_from_csv(filename)
  cleaned_sales_data = remove_sensitive_data(raw_sales_data)
  normalised_data = normalise_data(cleaned_sales_data)
  no_duplicate_products = no_duplicate_products(normalised_data)

  # load_data_into_db(no_duplicate_products, cleaned_sales_data)