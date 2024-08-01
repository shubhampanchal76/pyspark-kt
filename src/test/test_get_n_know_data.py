import pytest
import sys
from pyspark.sql import SparkSession
sys.path.append(r'C:\Users\Shubham\Downloads\Pyspark_KT\02_filtering_sorting_data')
from src.common_utilities.utils import *
import pandas as pd
import numpy as np

spark = SparkSession.builder.appName("Test").getOrCreate()


def test_get_unique():
    df = pd.DataFrame({"Gender" : ['M','F', 'M']})
    df.to_csv("test_df.csv", index=False)
    df = spark.read.csv("test_df.csv", header=True)
    result = get_unique(df, "Gender")
    assert result.count() == 2

def test_get_above():

    data = pd.DataFrame({'itemname': [f'item{i}' for i in range(1, 11)],
            'value': [35, 45, 55, 60, 65, 70, 75, 80, 50, 40]})
    data.to_csv("test_df.csv")
    df = spark.read.csv("test_df.csv", header=True)
    result = get_above(df, "value", 55)
    assert result.count() == 5

def test_get_below():
    data = pd.DataFrame({'itemname': [f'item{i}' for i in range(1, 11)],
            'value': [35, 45, 55, 60, 65, 70, 75, 80, 50, 40]})
    data.to_csv("test_df.csv")
    df = spark.read.csv("test_df.csv", header=True)
    result = get_below(df, "value", 55)
    assert result.count() == 4


def test_get_null_count():
    data = {'A': range(1, 11),
            'B': np.random.rand(10),
            'C': ['Value' + str(i) for i in range(1, 11)],
            'D': [np.nan if i % 2 == 0 else i for i in range(1, 11)]}
    df = pd.DataFrame(data=data)
    df.to_csv("test_df.csv", index=False)
    df = spark.read.csv("test_df.csv", header = True)
    nulls = get_null_count(df, "D")
    assert nulls == 5

def test_fill_nulls():
    df = spark.read.csv("test_df.csv", header = True)
    df = fill_nulls(df, {"D":10})
    nulls = get_null_count(df,"D")
    assert nulls == 0

def test_get_duplicate_counts():
    data = {'A': [1, 2, 3, 4, 5, 3, 2, 8, 9, 1], 
            'B': [11, 12, 13, 14, 15, 13, 12, 18, 19, 11],  
            'C': ['A', 'B', 'C', 'D', 'E', 'C', 'B', 'H', 'I', 'A'],  
            'D': [21, 22, 23, 24, 25, 23, 22, 28, 29, 21]}
    df = pd.DataFrame(data)
    df.to_csv("test_df.csv", index= False)
    df = spark.read.csv("test_df.csv", header= True)
    duplicates = get_duplicate_counts(df)
    assert duplicates==3

def test_drop_duplicates():
    data = {'A': [1, 2, 3, 4, 5, 3, 2, 8, 9, 1], 
            'B': [11, 12, 13, 14, 15, 13, 12, 18, 19, 11],  
            'C': ['A', 'B', 'C', 'D', 'E', 'C', 'B', 'H', 'I', 'A'],  
            'D': [21, 22, 23, 24, 25, 23, 22, 28, 29, 21]}
    df = pd.DataFrame(data)
    df.to_csv("test_df.csv", index= False)
    df = spark.read.csv("test_df.csv", header= True)
    unique_count = drop_duplicates(df).count()
    assert unique_count == df.count() - get_duplicate_counts(df)



test_get_unique()
test_get_above()
test_get_below()
test_get_null_count()
test_fill_nulls()
test_get_duplicate_counts()
test_drop_duplicates()




