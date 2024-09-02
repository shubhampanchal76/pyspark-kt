import pandas as pd
import numpy as np
import sys, os
sys.path.append(os.getcwd())
from src.common_utilities.utils import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, expr)
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import col, min as _min
from pyspark.sql.functions import stddev

import logging
from datetime import datetime

current_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = rf"logs/baby_names/Log_{current_timestamp}.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

logging.info("---File Run Started---")
spark = SparkSession.builder.appName("Spark").getOrCreate()
logging.info("--- Spark Session Created---")

url = 'https://raw.githubusercontent.com/guipsamora/pandas_exercises/master/06_Stats/US_Baby_Names/US_Baby_Names_right.csv'
data = pd.read_csv(url)
data.to_csv(r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\input_data\baby.csv", index = False)
df = spark.read.csv(r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\input_data\baby.csv", header=True)
logging.info("--- Datasets Loaded---")

df = df.drop('Unnamed: 0')
logging.info("Dropped Column :- Unnamed : 0")

gender_counts = df.groupBy('Gender').count()
gender_counts_pandas = gender_counts.toPandas()
gender_counts_pandas.to_csv(r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\output_data\baby_names\gender_counts.csv", index=False)
logging.info(r"Gender Counts have been saved to {r'C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\output_data\baby_names'} as gender_counts.csv")

names = df.groupBy('Name').agg(_sum('Count').alias('TotalCount'))
sorted_names = names.orderBy('TotalCount', ascending=False)
sorted_names = sorted_names.toPandas()
sorted_names.to_csv(r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\output_data\baby_names\sorted_names.csv", index=False)
logging.info(f"Sorted names data has been saved to : {r'C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\output_data\baby_names'} as 'sorted_names.csv")

logging.info(f"The unique names are : {names.count()}")

max_row = names.orderBy(col('TotalCount').desc()).first()
max_name = max_row['Name']
logging.info(f"The name with the maximum count is: {max_name}")

min_count = names.select(_min(col('TotalCount'))).first()[0]
min_count_names = names.filter(col('TotalCount') == min_count)
min_count_size = min_count_names.count()
logging.info(f"Number of names with the minimum count ({min_count}): {min_count_size}")

std_dev = names.select(stddev('TotalCount')).first()[0]
logging.info(f"Standard deviation of the 'TotalCount' column: {std_dev}")

summary_stats = names.describe('TotalCount')
summary_stats = summary_stats.toPandas()
summary_stats.to_csv(r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\output_data\baby_names\summary_stats.csv", index=False)
logging.info(f"Summary stats has been saved to {r'C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\output_data\baby_names'} as 'summary_stats.csv")
logging.info("---File Run Ended---")