import os
import csv
from pprint import pp
from datetime import datetime
import logging
import boto3
import psycopg2
from dotenv import load_dotenv,find_dotenv

# # Load environment variables from .env file
load_dotenv(find_dotenv())
user = os.getenv("user")
password = os.getenv("password")
database = os.getenv("database")
host = os.getenv("host")
port = os.getenv("port")

connection = psycopg2.connect(
    user = user, 
    password = password, 
    database= database, 
    host = host, 
    port = port
)

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

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

def extract_raw_data_from_csv(key, filename):
    raw_sales_data = []

    try:
        lines = filename['Body'].read().decode('utf-8').splitlines(True)
        field_names = ['order_date_time', 'branch_location','customer_name', 'order_items', 'total_payment', 'payment_type', 'card_number']
        reader = csv.DictReader(lines, field_names)
            
        raw_sales_data = []
        for row in reader:
            raw_sales_data.append(row)

    except Exception as err:
        print(f"An error occured: {str(err)}")

    # print(raw_sales_data)
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

def load_data_into_db(product_data, order_data, load_data):
  products_with_id = {}
  for product in product_data:
    sql_product = f'''
    INSERT INTO venus_schema.products(product,flavour,price)
    VALUES
    ('{product['product_name']}','{product['flavour']}','{product['product_price']}');

    SELECT MAX(product_id) FROM venus_schema.products;
    '''
    
    product_id = load_data(sql_product)
    product['product_id'] = product_id

    products_with_id[f"{product['product_name']} - {product['flavour']}"] = product_id

  LOGGER.info(f'Event structure: {products_with_id}')


  orders_with_id_list = []
  for order in order_data:
    sql_order = f'''
    INSERT INTO venus_schema.orders (order_date, branch_location, total_payment, payment_type)
    VALUES ('{order['order_date_time']}','{order['branch_location']}','{order['total_payment']}','{order['payment_type']}');

    SELECT MAX(order_id) FROM venus_schema.orders;
    '''
    
    order_id = load_data(sql_order)
    order['order_id'] = order_id
    orders_with_id_list.append(order)
    
    for order_item in order['order_items']:
      combined_key = f"{order_item['product_name']} - {order_item['flavour']}"
      if combined_key in products_with_id:

        product_id = products_with_id[combined_key]

        sql_prods_on_order = f'''
        INSERT INTO venus_schema.products_on_orders (order_id, product_id)
        VALUES ('{order_id}', '{product_id}');

        '''
        load_data(sql_prods_on_order)

  LOGGER.info(f'Event structure: {orders_with_id_list}')
        
  return orders_with_id_list
  
def handler(event, context):
  LOGGER.info(f'Event structure: {event}')

  bucket = 'de-x3-lle-venus'
  #filename = 'chesterfield_16-03-2022_09-00-00.csv'

  s3 = boto3.client('s3')
  key = '2022/3/16/chesterfield_16-03-2022_09-00-00.csv'
  filename = s3.get_object(Bucket=bucket, Key=key)

  raw_sales_data = extract_raw_data_from_csv(key, filename)
  cleaned_sales_data = remove_sensitive_data(raw_sales_data)
  normalised_data = normalise_data(cleaned_sales_data)
  cleaned_products = no_duplicate_products(normalised_data)

  load_data_into_db(cleaned_products, cleaned_sales_data, load_data)