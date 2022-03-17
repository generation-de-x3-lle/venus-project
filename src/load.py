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

  return orders_with_id_list
  
pp(load_data_into_db(no_duplicate_products, cleaned_sales_data))


# select orders.order_date, orders.branch_location, orders.total_payment, orders.payment_type, products.product, products.flavour, products.price
# from orders
# JOIN products_on_order ON orders.order_id = products_on_order.order_id
# JOIN products ON products_on_order.product_id = products.product_id
# order by orders.order_date