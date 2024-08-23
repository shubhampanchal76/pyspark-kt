import logging
import pandas as pd
from pyspark.sql import SparkSession
import sys, os

sys.path.append(os.getcwd())  # Assuming your utils module is in the same directory
# sys.path.append(r'C:\Users\Shubham\Downloads\Pyspark_KT\02_filtering_sorting_data')  


out_data_path = r"C:\Users\Shubham\Desktop\VSCODE\pyspark-kt\out_data"
file_path = "occupation"

from src.common_utilities.utils import *

# Configure logging
logging.basicConfig(
    filename='occupation.log',  # Log file name
    level=logging.INFO,  # Logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s %(levelname)s %(message)s'  # Log message format
)
logger = logging.getLogger(__name__)  # Get logger for this script

url = 'https://raw.githubusercontent.com/justmarkham/DAT8/master/data/u.user'
user = pd.read_csv(url, sep='|')
user.to_csv("user.csv", index=False)

spark = SparkSession.builder.appName('Test').getOrCreate()

logger.info("Dataset loaded successfully.")  # Log dataset loading

df = spark.read.csv("user.csv", header=True)

logger.info("Starting data processing...")  # Log processing start

#logger.info("Male ratio per occupation:")
male_ratio_by_occ=male_ratio_per_occupation(df)
file_name = "male_ratio_by_occ.csv"
male_ratio_by_occ = male_ratio_by_occ.toPandas()
male_ratio_by_occ= male_ratio_by_occ.to_csv(rf"{out_data_path}\{file_path}\{file_name}", index = False)
logger.info(f"Male ratio per occupation has been stored to {out_data_path} as {file_name}")






mean_by_age = calculate_mean_age_by_occupation(df)
file_name = "mean_by_age.csv"
mean_by_age= mean_by_age.toPanda()
mean_by_age=mean_by_age.to_csv(rf"{out_data_path}\{file_path}\{file_name}", index = False)
logger.info("Mean age by occupation has been stored to {out_data_path} as {file_name}")



min_max_age= min_max_age_per_occupation(df)
file_name= "min_max_age.csv"
min_max_age= min_max_age.toPandas()
min_max_age= min_max_age.to_csv(rf"{out_data_path}\{file_path}\{file_name}", index = False)
logger.info("Min max age by occupation has been stored to {out_data_path} as {file_name}")




mean_age_by_gender =mean_age_by_occupation_gender(df)
file_name= "mean_age_by_gender.csv"
mean_age_by_gender = mean_age_by_gender.toPandas()
mean_age_by_gender =mean_age_by_gender.to_csv(rf"{out_data_path}\{file_path}\{file_name}", index = False)
logger.info("Mean age by occupation by gender:  has been stored to {out_data_path} as {file_name}")

# logger.info("Gender percentage by occupation:")
# gender_percentage_per_occupation(df).show()  # Uncomment if needed

logger.info("Data processing completed.")  # Log processing completion

spark.stop()  # Close SparkSession (assuming you don't need it further)

