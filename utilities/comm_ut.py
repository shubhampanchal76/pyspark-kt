import pandas as pd
import numpy as np
from pyspark.sql.functions import trim, regexp_replace, col,desc
from pyspark.sql.types import FloatType

def change_item_price(df):
    df = df.withColumn("item_price", trim(regexp_replace(col("item_price"), "\\$", "")).cast(FloatType()))
    print(f"The item price has been changed to float")
    return df

def get_above(df, colm, value):
    return df.filter(col(colm)>value)

def get_below(df, colm,value):
    return df.filter(col(colm)<value)

def get_col_value(df, colm, value):
    return df.filter(col(colm)==value)

def sort_by(df, colm, order):
    if order == 0:
        return df.sort(colm)
    else:
        return df.sort(desc(colm))

def get_max(df, colm):
    c = sort_by(df,colm,1)
    max = c.limit(1)
    return max

def prod_order_qty(df,item):
    return get_col_value(df,'item_name',item).count()

def get_mult_order_count(df, item, qty):
    c = df.filter(col("item_name")==item)
    return c.filter(col("quantity")>=qty).count()

def show_col(df, colm):
    return df.select(colm)

def get_unique(df, colm):
    return df.select(colm).distinct()

def get_col_nums(df):
    return len(df.columns)

def sel_data(df, cols):
    return df.select(cols)

def get_teams_starts_with(df, letter):
    return df.filter(df.Team.startswith(letter)).select("Team")

def get_between(df,colm, upper, lower):
    return df.filter((col(colm) > upper) | (col("deaths") < lower))

def get_col_names(df):
    return df.columns