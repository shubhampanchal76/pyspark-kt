import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sys, os
sys.path.append(os.getcwd())
from src.common_utilities.utils import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
from pyspark.ml.feature import Bucketizer

import logging
from datetime import datetime

current_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = rf"logs/retail/Log_{current_timestamp}.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

logging.info("---File Run Started---")
spark = SparkSession.builder.appName("Spark").getOrCreate()
logging.info("--- Spark Session Created---")

url = "https://raw.githubusercontent.com/guipsamora/pandas_exercises/master/07_Visualization/Online_Retail/Online_Retail.csv"
data = pd.read_csv(url, encoding='latin1')
data.to_csv(r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\input_data\retail.csv", index=False)
df = spark.read.csv(r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\input_data\retail.csv", header=True)
logging.info("--- Datasets Loaded---")

countries = df.groupBy('Country').agg(_sum('Quantity').alias('TotalQuantity'))
countries_pandas = countries.toPandas()
countries_sorted = countries_pandas.sort_values(by='TotalQuantity', ascending=False)[1:11]
countries_sorted.to_csv(r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\output_data\retail\top10.csv", index=False)
logging.info(f"Top 10 countries data with most orders have been stored to : '{r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\output_data\retail"}' as top10.csv")
countries_sorted.set_index('Country')['TotalQuantity'].plot(kind='bar')
plt.xlabel('Countries')
plt.ylabel('Quantity')
plt.title('10 Countries with Most Orders')
plt.savefig(r'C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\output_data\retail\countries_with_most_orders.png')
plt.close()
logging.info(f"Top 10 countries plot with most orders have been stored to : '{r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\output_data\retail"}' as countries_with_most_orders.png")

customers = df.groupBy(['CustomerID', 'Country']).agg(
    _sum('Quantity').alias('Quantity'),
    _sum('UnitPrice').alias('UnitPrice')
)
customers_pandas = customers.toPandas()
if isinstance(customers_pandas.index, pd.MultiIndex):
    customers_pandas['Country'] = customers_pandas.index.get_level_values('Country')
else:
    # Handle the case if there is no multi-level index
    customers_pandas.reset_index(inplace=True)
    customers_pandas['Country'] = customers_pandas['Country']
top_countries = ['Netherlands', 'EIRE', 'Germany']
customers_filtered = customers_pandas[customers_pandas['Country'].isin(top_countries)]
g = sns.FacetGrid(customers_filtered, col="Country")
g.map(plt.scatter, "Quantity", "UnitPrice", alpha=1)
g.add_legend()
plt.savefig(r'C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\output_data\retail\scatter_plots_by_country.png', bbox_inches='tight')
plt.close()
logging.info(f"Scatter plot by countries has been saved to '{r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\output_data\retail"}' as scatter_plots_by_country.png")

df = df.withColumn("Quantity", col("Quantity").cast("double"))
df = df.withColumn("UnitPrice", col("UnitPrice").cast("double"))
df = df.withColumn("Revenue", col("Quantity") * col("UnitPrice"))
logging.info("Revenue Column calculated using Quantity and UnitPrice")



df = df.withColumn("UnitPrice", col("UnitPrice").cast("double"))
price_start = 0
price_end = 50
price_interval = 1
buckets = list(range(price_start, price_end + price_interval, price_interval))
bucketizer = Bucketizer(
    splits=[float('-inf')] + buckets + [float('inf')],
    inputCol='UnitPrice',
    outputCol='PriceBin'
)
df_binned = bucketizer.transform(df)
revenue_per_price = df_binned.groupBy('PriceBin').agg(_sum('Revenue').alias('TotalRevenue'))
revenue_per_price = revenue_per_price.orderBy('PriceBin')
revenue_per_price=revenue_per_price.toPandas()
revenue_per_price.to_csv(r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\output_data\retail\revenue_per_price.csv", index=False)
logging.info(f"Revenue per price data has been saved to : '{r'C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\output_data\retail'}' as revenue_per_price.csv")
revenue_per_price.plot()
plt.xlabel('Unit Price (in buckets of '+str(price_interval)+')') 
plt.ylabel('Revenue')
plt.xticks(np.arange(price_start,price_end,3),
           np.arange(price_start,price_end,3))
plt.yticks([0, 500000, 1000000, 1500000, 2000000, 2500000],
           ['0', '$0.5M', '$1M', '$1.5M', '$2M', '$2.5M'])
plt.savefig(r'C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\output_data\retail\revenue.png')
plt.close()
logging.info(f"Revenue plot as been saved to {r'C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\output_data\retail'}' as revenue.png")
logging.info("---File Run Ended---")