import csv
from pprint import pp

def extract_data_from_csv():
    sales_data = []
    with open('chesterfield_25-08-2021_09-00-00.csv','r') as file:
        # fields_names = ['order_date_time', 'branch_location',  'order_items', 'total_payment', 'payment_type']
        reader = csv.reader(file)

        for row in reader:
            sales_list = [row[0],row[1],row[3].split(','),row[4],row[5]]
            sales_data.append(sales_list)


    return sales_data
pp(extract_data_from_csv())