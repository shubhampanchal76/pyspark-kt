import pandas as pd
from pyspark.sql import SparkSession
import sys, os
sys.path.append(os.getcwd())
# sys.path.append(r'C:\Users\Shubham\Downloads\Pyspark_KT\02_filtering_sorting_data')
from src.common_utilities.utils import *
from src.common_utilities.utils import *

url = 'https://raw.githubusercontent.com/justmarkham/DAT8/master/data/drinks.csv'
con = pd.read_csv(url)
con.to_csv("Consumption.csv", index=False)

spark = SparkSession.builder.appName('Test').getOrCreate()

print("*"*75)
print("*"*75)
print("*"*75)

df = spark.read.csv("Consumption.csv", header=True)
print("The dataset has been loaded...")

df = convert_to_numeric(df, ['beer_servings','spirit_servings','wine_servings','total_litres_of_pure_alcohol'])
print("The required columns have been converted to numeric...")

print("The continent wise mean wine consumption is as follows :")
continent_by_avg_wine(df, 'continent','wine_servings','mean').show()

print("The dataframe group wise aggregates are as follows : ")
get_group_aggs(df, 'continent', 'wine_servings').show()

print("The group wise mean for each columns are as follows : ")
get_group_mean_by(df, 'continent').show()

print("The group wise median for each columns are as follows : ")
get_medians(df, 'continent').show()

print("The countinent wise Min MAx and Mean for Spirit servings are as follows : ")
get_group_MinMaxMean(df, 'continent', 'spirit_servings').show()

print("*"*75)
print("*"*75)
print("*"*75)