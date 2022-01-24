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

cities_dtype = [('id', 'bigint'), ('code', 'string'), ('city', 'string'), ('state', 'string')]
countries_dtype = [('id', 'bigint'), ('code', 'string'), ('country', 'string')]
city_demography_dtype = [('City', 'string'), ('State', 'string'), ('State Code', 'string'), ('Male Population', 'string'), ('Female Population', 'string'), ('Foreign-born', 'string'), ('Number of Veterans', 'string'), ('Race', 'string')]
city_stats_dtype = [('City', 'string'), ('State Code', 'string'), ('Median Age', 'string'), ('Average Household Size', 'string'), ('Count', 'string')]
airports_stats_dtype = [('ident', 'string'), ('elevation_ft', 'string'), ('coordinates', 'string')]
airports_dim_dtype = [('ident', 'string'), ('type', 'string'), ('name', 'string'), ('continent', 'string'), ('gps_code', 'string'), ('iata_code', 'string'), ('local_code', 'string'), ('iso_country', 'string')]
fact_immigration_dtype = [('cic_id', 'double'), ('year', 'double'), ('month', 'double'), ('arrival_date', 'date'), ('departure_date', 'date'), ('mode', 'double'), ('visatype', 'string')]

def check_table_schema(spark,output_path):
    print(Path(output_path))
    output_path = Path(output_path)
    for file in output_path.iterdir():
        if file.is_dir():
            if file == 'fact_immigration.parquet' and (spark.read.parquet(str(file)).dtypes != fact_immigration_dtype):
                raise ValueError("Fact Immigration Table schema not correct")
            elif file == 'airports_dim.parquet' and (spark.read.parquet(str(file)).dtypes != airports_dim_dtype):
                raise ValueError("Airports Dim Table schema not correct")
            elif file == 'airports_stats.parquet' and (spark.read.parquet(str(file)).dtypes != airports_stats_dtype):
                raise ValueError("Aiports Stats Table schema not correct")
            elif file == 'city_stats.parquet' and (spark.read.parquet(str(file)).dtypes != city_stats_dtype):
                raise ValueError("City Stats Table schema not correct")
            elif file == 'city_demography.parquet' and (spark.read.parquet(str(file)).dtypes != city_demography_dtype):
                raise ValueError("City Demography Table schema not correct")
            elif file == 'countries.parquet' and (spark.read.parquet(str(file)).dtypes != countries_dtype):
                raise ValueError("Countries Table schema not correct")
            elif file == 'cities.parquet' and (spark.read.parquet(str(file)).dtypes != cities_dtype):
                raise ValueError("Cities Table schema not correct")
            else:
                "Table Schemas correct"
                
def check_empty_tables(spark,output_path):
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
    check_table_schema(spark,output_path)
    check_empty_tables(spark,output_path)
