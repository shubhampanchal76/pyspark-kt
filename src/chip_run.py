import utils as ut
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession

print("*"*75)

spark = SparkSession.builder.appName("Test").getOrCreate()

url = 'https://raw.githubusercontent.com/justmarkham/DAT8/master/data/chipotle.tsv'
data = pd.read_csv(url, sep = '\t')
data.to_csv("chipotle_data.csv", index=False)

df = spark.read.csv("chipotle_data.csv", header=True)
print("The data has been Loaded")

df = ut.change_item_price(df)

price_10_above = ut.get_above(df,"item_price", 10)

chicken_bowl_prices = ut.get_col_value(df,'item_name','Chicken Bowl')
chicken_bowl_prices.show()

sorted_itemName = ut.sort_by(df,'item_name',0)
sorted_itemName.show()

max_item_price = ut.get_max(df, "item_price")
max_item_price.show()

veg_bowl_salad = ut.prod_order_qty(df,'Veggie Salad Bowl')
print(f"The product order quantity for Veggie Salad Bowl : {veg_bowl_salad}")

canned_soda_odr = ut.get_mult_order_count(df, "Canned Soda",2)
print(f"The order for Canned Sode for more than 2 qty are : {canned_soda_odr}")

print("*"*75)