import csv
from pprint import pp
from datetime import datetime

def extract_raw_data_from_csv():
    raw_sales_data = []

    try:
        with open('../data/chesterfield_25-08-2021_09-00-00.csv','r') as file:

            field_names = ['order_date_time', 'branch_location','customer_name', 'order_items', 'total_payment', 'payment_type', 'card_number']
            reader = csv.DictReader(file, field_names)
            
            raw_sales_data = []
            for row in reader:
                raw_sales_data.append(row)
    except Exception as err:
        print(f"An error occured: {str(err)}")

    return raw_sales_data

raw_sales_data = extract_raw_data_from_csv()

def remove_sensitive_data(raw_data):
    for item in raw_data:
        del item['customer_name']
        del item['card_number']
    return raw_data

cleaned_sales_data = remove_sensitive_data(raw_sales_data)

# pp(cleaned_sales_data)

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
      
      drink_order['order_items'] = order_items_list

    normalised_data_list.append(drink_order)

  return normalised_data_list

normalised_data = normalise_data(cleaned_sales_data)
#pp(normalised_data)

