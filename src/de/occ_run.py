import pandas as pd
from pyspark.sql import SparkSession
import sys
sys.path.append(r'C:\Users\Shubham\Downloads\Pyspark_KT\02_filtering_sorting_data')
from src.common_utilities.utils import *

url = 'https://raw.githubusercontent.com/justmarkham/DAT8/master/data/u.user'
user = pd.read_csv(url)
user.to_csv("Consumption.csv", index=False)

spark = SparkSession.builder.appName('Test').getOrCreate()

print("*"*75)
print("*"*75)
print("*"*75)

df = spark.read.csv(".csv", header=True)
print("The dataset has been loaded...")



print(" male ratio per occupation:")
male_ratio_per_occupation(df).show()

print(" mean age by occupation : ")
calculate_mean_age_by_occupation(df).show()


print("min max age by mean : ")
min_max_age_per_occupation(df).show


print(" mean age by occupation by gender: ")
mean_age_by_occupation_gender(df).show()


print(" gender percentage by occupation :")
gender_percentage_per_occupation(df).show()


