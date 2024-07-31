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


test_get_unique()
test_get_above()
test_get_below()




