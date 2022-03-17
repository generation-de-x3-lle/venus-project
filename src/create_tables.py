import db

create_cleaned_data_table = """
    CREATE TABLE IF NOT EXISTS clean_data(
    id serial NOT NULL PRIMARY KEY,
    order_date timestamp,
    branch_location varchar(50),
    products_on_order varchar(2500),
    total_payment decimal(19,2),
    payment_type varchar(20)
    );
    """


create_products_table = """
    CREATE TABLE IF NOT EXISTS products(
    product_id serial NOT NULL PRIMARY KEY,
    product varchar(100),
    flavour varchar(30),
    price decimal(19,2)
    );
    """

create_orders_table = """
    CREATE TABLE IF NOT EXISTS orders(
    order_id serial NOT NULL PRIMARY KEY,
    order_date timestamp,
    branch_location varchar(50),
    total_payment decimal(19,2),
    payment_type varchar(20)
    );
    """

create_products_on_order_table = """
    CREATE TABLE IF NOT EXISTS products_on_order(
    product_on_order_id serial NOT NULL PRIMARY KEY,
    order_id int NOT NULL,
    product_id int NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
    );
    """


db.create_table(create_cleaned_data_table, 'clean_data')
db.create_table(create_products_table, 'products')
db.create_table(create_orders_table, 'orders')
db.create_table(create_products_on_order_table, 'products_on_order')
