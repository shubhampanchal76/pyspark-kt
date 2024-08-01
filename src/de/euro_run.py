import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import sys, os
sys.path.append(os.getcwd())
# sys.path.append(r'C:\Users\Shubham\Downloads\Pyspark_KT\02_filtering_sorting_data')
from src.common_utilities.utils import *
from src.common_utilities.utils import *

print("*"*75)

spark = SparkSession.builder.appName("Test").getOrCreate()

url = 'https://raw.githubusercontent.com/guipsamora/pandas_exercises/master/02_Filtering_%26_Sorting/Euro12/Euro_2012_stats_TEAM.csv'
data = pd.read_csv(url)
data.to_csv("Euro_data.csv",index = False)

df = spark.read.csv("Euro_data.csv", header=True)

goals = show_col(df,"Goals")
print(f"The goal are : ")
goals.show()

uni_teams = get_unique(df,"Team")
print(f"The unique teams are : ")
uni_teams.show()

col_nums = get_col_nums(df)
print(f"The number of Columns in the dataset are : {col_nums}")

new_data = sel_data(df, ['Team', 'Yellow Cards', 'Red Cards'])
new_data.show()

sorted_red = sort_by(new_data, "Red Cards",1)
sored_red_yellow = sort_by(sorted_red,"Yellow Cards",1)
print(f"The dataframe sorted by Red card and yellow card : ")
sored_red_yellow.show()

print("The team that scored more than 6 goals are : ")
get_above(sel_data(df,['Team','Goals']),'Goals',6).show()

print("The teams that starts with G are : ")
get_teams_starts_with(df, "G").show()