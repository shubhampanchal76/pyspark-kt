import utils as ut
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# from utilities import comm_ut as ut

spark = SparkSession.builder.appName("Test").getOrCreate()

raw_data = {'regiment': ['Nighthawks', 'Nighthawks', 'Nighthawks', 'Nighthawks', 'Dragoons', 'Dragoons', 'Dragoons', 'Dragoons', 'Scouts', 'Scouts', 'Scouts', 'Scouts'],
            'company': ['1st', '1st', '2nd', '2nd', '1st', '1st', '2nd', '2nd','1st', '1st', '2nd', '2nd'],
            'deaths': [523, 52, 25, 616, 43, 234, 523, 62, 62, 73, 37, 35],
            'battles': [5, 42, 2, 2, 4, 7, 8, 3, 4, 7, 8, 9],
            'size': [1045, 957, 1099, 1400, 1592, 1006, 987, 849, 973, 1005, 1099, 1523],
            'veterans': [1, 5, 62, 26, 73, 37, 949, 48, 48, 435, 63, 345],
            'readiness': [1, 2, 3, 3, 2, 1, 2, 3, 2, 1, 2, 3],
            'armored': [1, 0, 1, 1, 0, 1, 0, 1, 0, 0, 1, 1],
            'deserters': [4, 24, 31, 2, 3, 4, 24, 31, 2, 3, 2, 3],
            'origin': ['Arizona', 'California', 'Texas', 'Florida', 'Maine', 'Iowa', 'Alaska', 'Washington', 'Oregon', 'Wyoming', 'Louisana', 'Georgia']}

data = pd.DataFrame(data=raw_data)
data.to_csv("Army.csv",index=False)

print("*"*75)

df = spark.read.csv("Army.csv", header=True)
print("The dataset has been loaded...")

print("The veterans col is as follows : ")
ut.show_col(df,"veterans").show()

print("The veterans and Deaths columns is as follows : ")
ut.show_col(df, ['veterans','deaths']).show()

col_names = ut.get_col_names(df)
print(f"The column names are : {col_names}")

maine_alaska = ut.show_col(df.filter((df.origin == "Maine") | (df.origin == "Alaska")), ['origin','deaths', 'size', 'deserters'])
maine_alaska.show()

deaths_above_50 = ut.get_above(df,'deaths', 50)
print("The data for deaths above 50 is : ")
deaths_above_50.show()

print("The rows for deaths above 500 and less than 50 are : ")
ut.get_between(df,"deaths", 500, 50).show()

print("*"*75)