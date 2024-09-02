import pandas as pd
import numpy as np
import sys, os
sys.path.append(os.getcwd())
from src.common_utilities.utils import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, expr)
from pyspark.sql.functions import monotonically_increasing_id
import logging

from datetime import datetime

# Get current timestamp
current_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
print(current_timestamp)
log_filename = rf"logs/cars/Log_{current_timestamp}.log"
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

logging.info("---File Run Started---")

spark = SparkSession.builder.appName("Spark").getOrCreate()
logging.info("--- Spark Session Created---")

url1 = "https://raw.githubusercontent.com/guipsamora/pandas_exercises/master/05_Merge/Auto_MPG/cars1.csv"
url2 = "https://raw.githubusercontent.com/guipsamora/pandas_exercises/master/05_Merge/Auto_MPG/cars2.csv"

cars1 = pd.read_csv(url1)
cars2 = pd.read_csv(url2)

cars1.to_csv(r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\input_data\Cars1.csv", index=False)
cars2.to_csv(r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\input_data\Cars2.csv", index=False)

df1 = spark.read.csv(r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\input_data\Cars1.csv", header=True)
df1 = df1.select("mpg", "cylinders", "displacement", "horsepower", "weight", "acceleration", "model", "origin", "car")
df2 = spark.read.csv(r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\input_data\Cars2.csv", header=True)
logging.info("--- Datasets Loaded---")

df1_obs = get_total_obs(df1)
logging.info(f"The observations in dataframe 1 are : {df1_obs}")
df2_obs = get_total_obs(df2)
logging.info(f"The observations in dataframe 1 are : {df2_obs}")

df = append_dfs(df1,df2)
logging.info("The 2 dataframes are merged...")

nr_owners_df = spark.range(398).select(
    (expr("cast(rand() * (73001 - 15000) + 15000 as long)").alias("nr_owners"))
)
logging.info("The Owners column has been created...")

df = df.withColumn("id", monotonically_increasing_id())
nr_owners_df = nr_owners_df.withColumn("id", monotonically_increasing_id())

# Join the DataFrames on the index
df = df.join(nr_owners_df, on="id").drop("id")

# Rename the column in the joined DataFrame
df = df.withColumnRenamed("nr_owners", "owners")
logging.info("The owners column has been merged...")
df = df.toPandas()
df.to_csv(r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\data\output_data\Cars.csv", index=False)
logging.info("The Final Dataframe has been stored to the output folder...")
logging.info("---The File Run has Ended---")
