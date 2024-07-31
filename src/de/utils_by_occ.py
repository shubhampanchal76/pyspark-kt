
from pyspark.sql.functions import col, count, sum
from pyspark.sql.functions import col, count, when
import pandas as pd
import numpy as np
from pyspark.sql.functions import trim, regexp_replace, col,desc
from pyspark.sql.functions import sum as pyspark_sum
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F
from pyspark.sql.functions import count, mean, stddev, min, max, median
from pyspark.sql.types import FloatType, DoubleType, IntegerType
import pyspark.sql.functions as Fom pyspark.sql.functions import col,aggregate







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
  result= df.filter(regiment.regiment == colm).selectExpr("avg(preTestScore)").collect()[0][0]
  return result





def mean_pretestscore_company(df,grp_colm):
   mean = df.groupBy(grp_colm).agg(F.mean("preTestScore")).show()
   return mean


def mean_pretestscore_regiment_company(df):
  mean = df.groupBy("regiment", "company").agg(F.mean("preTestScore")).show()
  return mean

def regiment_company_count(df):
  result=df.groupBy("regiment", "company").count().show()
  return result


def group_by_regiment_company(df):
 result=df.groupBy("regiment", "company").mean().show()
 return result
