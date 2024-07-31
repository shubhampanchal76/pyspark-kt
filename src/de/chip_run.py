# import common_utilities.utils as ut
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import sys
sys.path.append(r'C:\Users\Shubham\Downloads\Pyspark_KT\02_filtering_sorting_data')
from src.common_utilities.utils import *
spark = SparkSession.builder.appName("Test").getOrCreate()

url = 'https://raw.githubusercontent.com/justmarkham/DAT8/master/data/chipotle.tsv'
data = pd.read_csv(url, sep = '\t')
data.to_csv("chipotle_data.csv", index=False)

print("*"*75)
print("*"*75)
print("*"*75)

df = spark.read.csv("chipotle_data.csv", header=True)
print("The data has been Loaded")

df = fill_missing(df, "choice_description", "[Other]")

show_n_rows(df,5)

total_observations = get_total_obs(df)
print(f"The total number of observations are : {total_observations}")

total_columns = get_columns_nums(df)
print(f"The total Num of Columns in the dataset are : {total_columns}")

col_names = get_col_names(df)
print(f"The column names in the dataset are : {col_names}")

mst_ord_item = get_most_ordered_item(df)
print(f"The most Ordered Item is : {mst_ord_item}")

most_ordered_item_qty = int(get_most_ordered_item_qty(df))
print(F"The Mosrt Ordered Item Quantity is : {most_ordered_item_qty}")

most_ordered_choice_item = get_most_ordered_choice_item(df)
print(f"The most ordered choice item is : {most_ordered_choice_item}")

total_qty = get_total_quantities(df)
print(f"Total Quantities are : {total_qty}")

df = change_item_price(df)

total_revenue = get_total_revenue(df)
print(f"The total revenue is : {total_revenue}")

total_orders = get_total_orders(df)
print(f"The total orders are : {total_orders}")

avg_order_value = get_avg_order_value(revenue=total_revenue, total_orders=total_orders)
print(f"The average order value is : ${avg_order_value}")

unique_item_count = get_unique_item_cnt(df)
print(f"The unique item count is : {unique_item_count}")

# df = change_item_price(df)

price_10_above = get_above(df,"item_price", 10)

chicken_bowl_prices = get_col_value(df,'item_name','Chicken Bowl')
chicken_bowl_prices.show()

sorted_itemName = sort_by(df,'item_name',0)
sorted_itemName.show()

max_item_price = get_max(df, "item_price")
max_item_price.show()

veg_bowl_salad = prod_order_qty(df,'Veggie Salad Bowl')
print(f"The product order quantity for Veggie Salad Bowl : {veg_bowl_salad}")

canned_soda_odr = get_mult_order_count(df, "Canned Soda",2)
print(f"The order for Canned Sode for more than 2 qty are : {canned_soda_odr}")

print("*"*75)
print("*"*75)
print("*"*75)