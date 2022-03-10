import csv
from pprint import pp

def extract_raw_data_from_csv():
    raw_sales_data = []

    try:
        with open('data/chesterfield_25-08-2021_09-00-00.csv','r') as file:

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
  for drink_item in cleaned_data:
    drink_item['order_items'] = drink_item['order_items'].split(',')
    
    #Creating a list of dictionaries for order_items
    order_items_list = []
    for order_item in drink_item['order_items']:
      drink = order_item.rsplit("-", 1)[0]
      price = order_item.rsplit("-", 1)[1]
    
      print(f"No flavour drink: {drink} and price: {price}")
      hot_drink = {
        "product_name" : drink,
        "product_price" : price
      }

      order_items_list.append(hot_drink)
      
      drink_item['order_items'] = order_items_list

    normalised_data_list.append(drink_item)

  return normalised_data_list

normalised_data = normalise_data(cleaned_sales_data)
pp(normalised_data)

