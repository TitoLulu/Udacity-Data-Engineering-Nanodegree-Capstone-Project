from datetime import datetime, timedelta
from email import header
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col, array_contains, split, monotonically_increasing_id
from pyspark.sql.types import DateType, StringType
import pandas as pd
import os
import configparser
from pathlib import Path

config = configparser.ConfigParser()
config.read('capstone.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['aws_cred']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['aws_cred']['AWS_SECRET_ACCESS_KEY']

def createsparksession():
    spark = SparkSession.builder\
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()

    return spark

def check_table_schema(output_path):
    print(Path(output_path))
    output_path = Path(output_path)
    for file in output_path.iterdir():
        if file.is_dir():
            df = spark.read.parquet(str(file))
            print("Schema: " + df.split('/')[-1])
            df.printSchema()
                
def check_empty_tables(output_path):
    output_path = Path(output_path)
    for file in output_path.iterdir():
        if file.is_dir():
            df = spark.read.parquet(str(file)).count()
            if df == 0:
                print("Empty Table: ", df.split('/')[-1])
            else:
                print("Table: ", df.split('/')[-1], "is not empty")
            
    
    
if __name__ == '__main__':
    spark = createsparksession()
    output_path = ""
    check_table_schema(output_path)
    check_empty_tables(output_path)