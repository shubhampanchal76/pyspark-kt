import pandas as pd
import numpy as np
from pyspark.sql.functions import trim, regexp_replace, col,desc
from pyspark.sql.functions import sum as pyspark_sum
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F
from pyspark.sql.functions import count, mean, stddev, min, max, median
from pyspark.sql.types import FloatType, DoubleType, IntegerType
import pyspark.sql.functions as Fom pyspark.sql.functions import col,aggregate

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

def fill_missing(df,col_name, value):
    df = df.fillna({col_name:value})
    print(f"The missing values in {col_name} are replaced with {value}")
    return df

# For Filling missing values
def fill_missing(df,col_name, value):
    df = df.fillna({col_name:value})
    print(f"The missing values in {col_name} are replaced with {value}")
    return df
    
# For displaying first n rows of pyspark dataframe
def show_n_rows(df,n):
    print(f"The first {n} rows are : ")
    df.show(n)

# for getting the number of observation in pyspark df
def get_total_obs(df):
    return df.count()

# for getting total number of columns in dataset
def get_columns_nums(df):
    return len(df.columns)

# for getting the column names
def get_col_names(df):
    return df.columns

# for getting most ordered item
def get_most_ordered_item(df):
    c = df.groupBy("item_name").agg(pyspark_sum("quantity").alias("total_quantity"))
    c = c.orderBy(col("total_quantity").desc())
    top_item = c.limit(1)
    return top_item.head(1)[0][0]

def get_most_ordered_item_qty(df):
    c = df.groupBy("item_name").agg(pyspark_sum("quantity").alias("total_quantity"))
    c = c.orderBy(col("total_quantity").desc())
    top_item = c.limit(1)
    return top_item.head(1)[0][1]

# for most ordered item in choice description
def get_most_ordered_choice_item(df):
    c = df.groupBy("choice_description").agg(pyspark_sum("quantity").alias("total_quantity"))
    c = c.orderBy(col("total_quantity").desc())
    top_item = c.limit(1)
    return top_item.head(1)[0][0]

# for getting total quantities ordered
def get_total_quantities(df):
    qty = int(df.agg({"quantity" :"sum"}).collect()[0][0])
    return qty

# for converting item_price
def change_item_price(df):
    df = df.withColumn("item_price", trim(regexp_replace(col("item_price"), "\\$", "")).cast(FloatType()))
    print(f"The item price has been changed to float")
    return df
# for getting total revenue
def get_total_revenue(df):
    df = df.withColumn('revenue', df['quantity'] * df['item_price'])
    return round(df.select(F.sum('revenue')).collect()[0][0],2)

# for getting total orders
def get_total_orders(df):
    return df.select('order_id').distinct().count()

# for getting average order value
def get_avg_order_value(revenue, total_orders):
    return round((revenue/total_orders),2)

def get_unique_item_cnt(df):
    return df.select('item_name').distinct().count()
##

def convert_to_numeric(df, columns):
    for column in columns:
        df = df.withColumn(column, col(column).cast(FloatType()))
    return df

def continent_by_avg_wine(df, col1, col2, agg_method):
    return df.groupBy(col1).agg({col2: agg_method})

def get_group_aggs(df, col1, col2):
    return df.groupBy(col1).agg(
    count(col2).alias('count'),
    mean(col2).alias('mean'),
    stddev(col2).alias('stddev'),
    min(col2).alias('min'),
    max(col2).alias('max')
    )

def get_group_mean_by(df, colm):
    return df.groupBy('continent').mean()

def get_medians(df, grp_col):
    numeric_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, (FloatType, DoubleType, IntegerType))]
    
    # Filter out 'continent' from numeric columns if it exists
    numeric_columns = [col for col in numeric_columns if col != grp_col]
    
    agg_expressions = [median(col).alias(f'mean_{col}') for col in numeric_columns]
    
    return df.groupBy(grp_col).agg(*agg_expressions)

def get_group_MinMaxMean(df, col1, col2):
  return df.groupBy(col1).agg(
        F.mean(col2).alias(f"mean_{col2}"),
        F.min(col2).alias(f"min_{col2}"),
        F.max(col2).alias(f"max_{col2}"))