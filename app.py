import csv
from pprint import pp

def extract_raw_data_from_csv():
    sales_data = []

    try:
        with open('chesterfield_25-08-2021_09-00-00.csv','r') as file:
            field_names = ['order_date_time', 'branch_location','customer_name', 'order_items', 'total_payment', 'payment_type', 'card_number']
            reader = csv.DictReader(file, field_names)
            
            raw_sales_data = []
            for row in reader:
                raw_sales_data.append(row)
    except Exception as err:
        print(f"An error occured: {str(err)}")

    return raw_sales_data

raw_sales_data = extract_raw_data_from_csv()
