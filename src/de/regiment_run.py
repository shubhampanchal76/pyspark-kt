import pandas as pd
from pyspark.sql import SparkSession
import sys, os
sys.path.append(os.getcwd())
# sys.path.append(r'C:\Users\Shubham\Downloads\Pyspark_KT\02_filtering_sorting_data')
from src.common_utilities.utils import *
import boto3
import yaml
import logging


logging.basicConfig(filename='logger.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

logging.info("---File Run Started---")

config_path = 'C:/Users/Shubham/Desktop/VSCODE/pyspark-kt/src/config/config.yaml'
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

aws_access_key_id = config["aws"]["aws_access_key_id"]
aws_secret_access_key = config["aws"]["aws_secret_access_key"]
region_name = config["aws"]["aws_region"]
aws_session_token = config["aws"]["aws_session_token"]
logging.info("Aws Configured...")

input_file_name = "Regiment.csv"
s3_client = boto3.client(
    's3',
    aws_access_key_id= aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name,
    aws_session_token=aws_session_token
)
logging.info("S3 Client Configured")

bucket_name = config["aws"]["s3_bucket_name"]
input_data_folder = config["aws"]["input_data_folder"]
input_file_path = f"{input_data_folder}/{input_file_name}"
output_data_folder = config["aws"]["output_data_folder"]
logging.info("S3 Data Paths Configured")


s3_client.create_bucket(Bucket=bucket_name)
logging.info(f"S3 Bucket '{bucket_name}' created")



raw_data = {'regiment': ['Nighthawks', 'Nighthawks', 'Nighthawks', 'Nighthawks', 'Dragoons', 'Dragoons', 'Dragoons', 'Dragoons', 'Scouts', 'Scouts', 'Scouts', 'Scouts'],
        'company': ['1st', '1st', '2nd', '2nd', '1st', '1st', '2nd', '2nd','1st', '1st', '2nd', '2nd'],
        'name': ['Miller', 'Jacobson', 'Ali', 'Milner', 'Cooze', 'Jacon', 'Ryaner', 'Sone', 'Sloan', 'Piger', 'Riani', 'Ali'],
        'preTestScore': [4, 24, 31, 2, 3, 4, 24, 31, 2, 3, 2, 3],
        'postTestScore': [25, 94, 57, 62, 70, 25, 94, 57, 62, 70, 62, 70]}


regiment = pd.DataFrame(raw_data, columns = raw_data.keys())
file_name = input_file_name
local_file_path = f"data/input_data/{file_name}"
regiment.to_csv(local_file_path, index= False)
s3_client.upload_file(local_file_path, bucket_name, input_file_path)
# s3_client.put_object(Bucket=bucket_name, Key=input_file_path, Body=csv_buffer.getvalue())
logging.info(f"'{file_name}' uploaded successfully to '{input_file_path}' in bucket '{bucket_name}'.")


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkBy').getOrCreate()

df = spark.read.csv(local_file_path, header = True)
logging.info("The dataset has been loaded...")

logging.info(f"mean pretestscore  of nighthawks: {mean_pretestscore_nighthawks(df,"Nighthawks")}")


# print(" pretestscore by company : ")
mean_pretest_company = mean_pretestscore_company(df,"company")
# mean_pretest_company.show()
mean_pretest_company = mean_pretest_company.toPandas()
file_name = "mean_pretest_company.csv"
local_file_path = f"data/output_data/{file_name}"
mean_pretest_company.to_csv(local_file_path, index=False)
output_file_path = f"{output_data_folder}/{file_name}"
s3_client.upload_file(local_file_path, bucket_name, output_file_path)
logging.info(f"'{file_name}' uploaded successfully to '{output_file_path}' in bucket '{bucket_name}'.")

# print(" mean prescore by regiment and company :")
mean_pretestscore_reg_comp =mean_pretestscore_regiment_company(df)
mean_pretestscore_reg_comp = mean_pretestscore_reg_comp.toPandas()
file_name = "mean_pretestscore_reg_comp.csv"
local_file_path = f"data/output_data/{file_name}"
mean_pretestscore_reg_comp.to_csv(local_file_path, index=False)
output_file_path = f"{output_data_folder}/{file_name}"
s3_client.upload_file(local_file_path, bucket_name, output_file_path)
logging.info(f"'{file_name}' uploaded successfully to '{output_file_path}' in bucket '{bucket_name}'.")




# print("regiment company count :")
regiment_company_cnt = regiment_company_count(df)
regiment_company_cnt = regiment_company_cnt.toPandas()
file_name = "regiment_company_cnt.csv"
local_file_path = f"data/output_data/{file_name}"
regiment_company_cnt.to_csv(local_file_path, index=False)
output_file_path = f"{output_data_folder}/{file_name}"
s3_client.upload_file(local_file_path, bucket_name, output_file_path)
logging.info(f"'{file_name}' uploaded successfully to '{output_file_path}' in bucket '{bucket_name}'.")



# print(" group by regiment company :")
group_by_regiment_comp = group_by_regiment_company(df)
group_by_regiment_comp = group_by_regiment_comp.toPandas()
file_name = "group_by_regiment_comp.csv"
local_file_path = f"data/output_data/{file_name}"
group_by_regiment_comp.to_csv(local_file_path, index=False)
output_file_path = f"{output_data_folder}/{file_name}"
s3_client.upload_file(local_file_path, bucket_name, output_file_path)
logging.info(f"'{file_name}' uploaded successfully to '{output_file_path}' in bucket '{bucket_name}'.")


logging.info("---File Run Stopped---")