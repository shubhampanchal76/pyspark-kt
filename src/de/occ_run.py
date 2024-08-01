import pandas as pd
from pyspark.sql import SparkSession
from utils_by_occ import *

url = 'https://raw.githubusercontent.com/justmarkham/DAT8/master/data/u.user'
user = pd.read_csv(url, sep='|')
user.to_csv("user.csv", index=False)

spark = SparkSession.builder.appName('Test').getOrCreate()

print("*"*75)
print("*"*75)
print("*"*75)

df = spark.read.csv("user.csv", header=True)
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


print("*"*75)
print("*"*75)
print("*"*75)