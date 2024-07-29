import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import utils as ut
from utilities import comm_ut as ut

print("*"*75)

spark = SparkSession.builder.appName("Test").getOrCreate()

url = 'https://raw.githubusercontent.com/guipsamora/pandas_exercises/master/02_Filtering_%26_Sorting/Euro12/Euro_2012_stats_TEAM.csv'
data = pd.read_csv(url)
data.to_csv("Euro_data.csv",index = False)

df = spark.read.csv("Euro_data.csv", header=True)

goals = ut.show_col(df,"Goals")
print(f"The goal are : ")
goals.show()

uni_teams = ut.get_unique(df,"Team")
print(f"The unique teams are : ")
uni_teams.show()

col_nums = ut.get_col_nums(df)
print(f"The number of Columns in the dataset are : {col_nums}")

new_data = ut.sel_data(df, ['Team', 'Yellow Cards', 'Red Cards'])
new_data.show()

sorted_red = ut.sort_by(new_data, "Red Cards",1)
sored_red_yellow = ut.sort_by(sorted_red,"Yellow Cards",1)
print(f"The dataframe sorted by Red card and yellow card : ")
sored_red_yellow.show()

print("The team that scored more than 6 goals are : ")
ut.get_above(ut.sel_data(df,['Team','Goals']),'Goals',6).show()

print("The teams that starts with G are : ")
ut.get_teams_starts_with(df, "G").show()