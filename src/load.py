from app import normalised_data
import db

def load_data_into_db(data):
    for row in data:
        products = row.get('order_items')
        for product in products:
            
            sql = f'''
            INSERT INTO products(product,flavour_id,price)
            VALUES
            ('{product['product_name']}','{product['flavour']}','{product['product_price']}')
            ON CONFLICT ON CONSTRAINT '{product['flavour']}','{product['product_name']}'
            DO  NOTHING
            '''
            db.load_data(sql)

load_data_into_db(normalised_data)



