from app import normalised_data, no_duplicate_products
import db
from pprint import pp

def load_data_into_db(data):
  for product in data:
      
      sql = f'''
      INSERT INTO products(product,flavour_id,price)
      VALUES
      ('{product['product_name']}','{product['flavour']}','{product['product_price']}')
      ON CONFLICT (product_id)
      DO  NOTHING
      '''
      db.load_data(sql)
  
load_data_into_db(no_duplicate_products)



