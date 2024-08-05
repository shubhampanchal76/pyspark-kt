import pandas as pd
import numpy as np
from pyspark.sql.functions import trim, regexp_replace, col,desc
from pyspark.sql.functions import sum as pyspark_sum
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F
from pyspark.sql.functions import count, mean, stddev, min, max, median
from pyspark.sql.types import FloatType, DoubleType, IntegerType
import pyspark.sql.functions as F
from pyspark.sql.functions import col,aggregate

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
    
    agg_expressions = [median(col).alias(f'median_{col}') for col in numeric_columns]
    
    return df.groupBy(grp_col).agg(*agg_expressions)

def get_group_MinMaxMean(df, col1, col2):
  return df.groupBy(col1).agg(
        F.mean(col2).alias(f"mean_{col2}"),
        F.min(col2).alias(f"min_{col2}"),
        F.max(col2).alias(f"max_{col2}"))

#

def male_ratio_per_occupation(df):
  
 
  from pyspark.sql.functions import col, count, when
  male_ratio = df.groupBy('occupation') \
                      .agg(count('*').alias('total'),
                           count(when(col('gender') == 'M', True)).alias('male_count')) \
                      .withColumn('male_ratio', col('male_count') / col('total')) \
                      .orderBy(col('male_ratio').desc())
  return male_ratio



#


def min_max_age_per_occupation(df):
 
  age_range = df.groupBy('occupation') \
                      .agg(F.min('age').alias('min_age'),
                           F.max('age').alias('max_age'))
  return age_range



def mean_age_by_occupation_gender(df):
  
  mean_age_df = df.groupBy("occupation","gender").agg({'age': 'mean'})
  return mean_age_df




def gender_percentage_per_occupation(df):
  
  gender_counts = df.groupBy('occupation', 'gender').agg(count('*').alias('count'))
  occupation_totals = gender_counts.groupBy('occupation').agg(sum('count').alias('total'))
  gender_percentages = gender_counts.join(occupation_totals, 'occupation') \
                                   .withColumn('percentage', (col('count') / col('total')) * 100) \
                                   .select('occupation', 'gender', 'percentage')
  return gender_percentages


def calculate_mean_age_by_occupation(df):
  

  mean_age_df = df.groupBy("occupation").agg(F.mean("age").alias("mean_age"))
  return mean_age_df



##


def mean_pretestscore_nighthawks(df,colm):
  return df.filter(df.regiment == colm).selectExpr("avg(preTestScore)").collect()[0][0]





def mean_pretestscore_company(df,grp_colm):
   return df.groupBy(grp_colm).agg(F.mean("preTestScore"))
   


def mean_pretestscore_regiment_company(df):
  return df.groupBy("regiment", "company").agg(F.mean("preTestScore"))
  

def regiment_company_count(df):
  return df.groupBy("regiment", "company").count()



def group_by_regiment_company(df):
 return df.groupBy("regiment", "company").mean()

def get_null_count(df, colm):
    return df.filter(df[colm].isNull()).count()

def get_duplicate_counts(df):
    return  df.groupBy(df.columns).count().filter("count > 1").count()

def drop_duplicates(df):
    return df.dropDuplicates()

def fill_nulls(df, dicts_val):
    return df.fillna(dicts_val)
