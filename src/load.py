from app import normalised_data, no_duplicate_products, cleaned_sales_data
import db
from pprint import pp

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


  return product_with_id_list
  


    # sql2 = f'''
    #   INSERT INTO order(order_date, branch_location, total_payment, payment_type)
    #   VALUES
    #   ('{}')
    # '''

      
  
pp(load_data_into_db(no_duplicate_products))



