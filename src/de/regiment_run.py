import pandas as pd
from pyspark.sql import SparkSession
import sys
sys.path.append(r'C:\Users\Shubham\Downloads\Pyspark_KT\02_filtering_sorting_data')
from src.common_utilities.utils import *

raw_data = {'regiment': ['Nighthawks', 'Nighthawks', 'Nighthawks', 'Nighthawks', 'Dragoons', 'Dragoons', 'Dragoons', 'Dragoons', 'Scouts', 'Scouts', 'Scouts', 'Scouts'],
        'company': ['1st', '1st', '2nd', '2nd', '1st', '1st', '2nd', '2nd','1st', '1st', '2nd', '2nd'],
        'name': ['Miller', 'Jacobson', 'Ali', 'Milner', 'Cooze', 'Jacon', 'Ryaner', 'Sone', 'Sloan', 'Piger', 'Riani', 'Ali'],
        'preTestScore': [4, 24, 31, 2, 3, 4, 24, 31, 2, 3, 2, 3],
        'postTestScore': [25, 94, 57, 62, 70, 25, 94, 57, 62, 70, 62, 70]}


regiment = pd.DataFrame(raw_data, columns = raw_data.keys())
regiment

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkBy').getOrCreate()
regiment = spark.createDataFrame(data=regiment)





print("mean pretestscore  of nighthawks:")
mean_pretestscore_nighthawks(df,colm).show


print(" pretestscore by company : ")
mean_pretestscore_company(df,grp_colm).show()

print(" mean prescore by regiment and company :")
mean_pretestscore_regiment_company(df).show()

print("regiment company count :")
regiment_company_count(df).show()

print(" group by regiment company :")
group_by_regiment_company(df).show()

