import csv

def extract_data_from_csv():
    sales_data = []
    with open('chesterfield_25-08-2021_09-00-00.csv','r') as file:
        fields_names = ['order_date_time', 'branch_location', 'customer_name', 'order', 'total_payment', 'payment_type', 'card_number']
        reader = csv.DictReader(file, fields_names)

        for row in reader:
            sales_data.append(row)

    print(sales_data)

extract_data_from_csv()