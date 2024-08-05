import pandas as pd
from pyspark.sql import SparkSession
import sys, os
sys.path.append(os.getcwd())
# sys.path.append(r'C:\Users\Shubham\Downloads\Pyspark_KT\02_filtering_sorting_data')  # Remove if not needed
from src.common_utilities.utils import *

import logging

# Configure logging (choose a suitable destination for your needs)
logging.basicConfig(filename='pyspark_consumption.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

url = 'https://raw.githubusercontent.com/justmarkham/DAT8/master/data/drinks.csv'

try:
    con = pd.read_csv(url)
    con.to_csv("Consumption.csv", index=False)
    logging.info("CSV data successfully downloaded and saved as Consumption.csv")
except Exception as e:
    logging.error("Error downloading or saving CSV data:", exc_info=True)
    sys.exit(1)  # Indicate failure

spark = SparkSession.builder.appName('Test').getOrCreate()

logging.info("SparkSession created successfully")

df = spark.read.csv("Consumption.csv", header=True)
logging.info("Data loaded into Spark DataFrame")

df = convert_to_numeric(df, ['beer_servings','spirit_servings','wine_servings','total_litres_of_pure_alcohol'])
logging.info("Required columns converted to numeric")

logging.info(f"Continent-wise mean wine consumption:")
continent_by_avg_wine(df, 'continent','wine_servings','mean')

logging.info(f"DataFrame group wise aggregates:")
get_group_aggs(df, 'continent', 'wine_servings')

logging.info(f"Group-wise mean for each column:")
get_group_mean_by(df, 'continent')

logging.info(f"Group-wise median for each column:")
get_medians(df, 'continent')

logging.info(f"Continent-wise Min, Max, and Mean for spirit servings:")
get_group_MinMaxMean(df, 'continent', 'spirit_servings')

spark.stop()
logging.info("SparkSession stopped")
