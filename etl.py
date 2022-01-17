from datetime import datetime, timedelta
from email import header
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType
import pandas as pd
import os
import configparser

config = configparser.ConfigParser()
config.read('capstone.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['aws_cred']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['aws_cred']['AWS_SECRET_ACCESS_KEY']

def createsparksession():
    spark = (
        SparkSession.builder.config(
            "spark.jars.repositories", "https://repos.spark-packages.org/"
        )
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")
        .enableHiveSupport()
        .getOrCreate()
    )

    return spark


def sas_to_date(date):
    """
    Format date columns from sas to datetime
    date: sas input type
    """
    # 1. sas dates to python datetime
    """
        datetime.strptime('2016-01-01', '%Y-%m-%d') - timedelta(20566) gives a date between the year 1959 and 1960 
        working with the date 1960-01-01 gives a more accurate date that reflects the arrival and departure dates
    """
    return (datetime.strptime("1960-01-01", "%Y-%m-%d") + timedelta(date)).strftime(
        "%Y-%m-%d"
    )


def check_missing_data(df):
    """
    Takes dataframe and checks for existence of USA temperature data
    """
    us_array = ["usa", "united states of america", "us"]
    df["Country"] = df.Country.apply(lambda x: x.lower())
    return df.Country.isin(us_array).unique()


def process_immigration_data(spark, input_data, output_data):
    """
     Loads immigration data and processes it into a fact table
     input_data: source of data
     output_data: destination storage for processed data
    """
    # fetch path fo data source
    immigration_data = os.path.join(
        input_data + "sas_data"
    )

    # load data
    immigration_df = spark.read.parquet(immigration_data)
    immigration_df.take(10)

    # select columns to work with
    immigration_df = immigration_df.select("*")
    # rename columns to make them inteligible
    new_cols =["cic_id","year","month","cit","res","port","arrival_date","mode","address","departure_date","age","visa","count","date_logged","dept_visa_issuance","occupation","arrival_flag","departure_flag","update_flag","match_flag","birth_year","max_stay_date","gender","INS_number","airline","admission_number","flight_number","visatype"]
    #create an iterator between the current df columns and new columns to bulk rename the columns in 
    #immigration_df
    for col, new_col in zip(immigration_df.columns, new_cols):
        immigration_df = immigration_df.withColumnRenamed(col, new_col) 


    immigration_fact = immigration_df[
        [
            "cic_id",
            "year",
            "month",
            "arrival_date",
            "departure_date",
            "mode",
            "visatype",
        ]
    ]
    # clean the dates
    udf_func = udf(sas_to_date,DateType())
    immigration_fact = immigration_fact.withColumn("arrival_date",udf_func("arrival_date"))
    immigration_fact = immigration_fact.withColumn("departure_date",udf_func("departure_date"))
    print(immigration_fact)

    immigration_fact.write.parquet(output_data + "immigration_fact.parquet")


def process_city_data(spark, input_data, output_data):
    """
    loads and processes city data
    input_data: path to data source
    output_data: path to data store
    """

    # fetch data path
    city_data = os.path.join(input_data + "us-cities-demographics.csv")

    # load city data
    city_df = spark.read.load(city_data, sep=";", format="csv", header=True)

    # Demography, examine the city based on size, and structure (Gender & Ethnicity composition)
    city_demography = city_df.select(
        [
            "City",
            "State",
            "State Code",
            "Male Population",
            "Female Population",
            "Foreign-born",
            "Number of Veterans",
            "Race",
        ]
    )
    city_demography.write.parquet(output_data + city_demography)

    # statistics on city such as total population, and median age
    city_stats = city_df.select(
        ["City", "State Code", "Median Age", "Average Household Size", "Count"]
    )
    city_stats.write.parquet(output_data + "city_stats.parquet")


def process_airport_data(spark, input_data, output_data):
    """
    loads and processes city data
    input_data: path to data source
    output_data: path to data store
    """

    # fetch data source
    airport_data = os.path.join(input_data + "airport-codes_csv.csv")
    # load data
    airport_df = spark.read.load(airport_data, sep=";", format="csv", header=True)

    airport_df = airport_df.select(
        [
            "ident",
            "type",
            "name",
            "continent",
            "gps_code",
            "iata_code",
            "local_code",
            "iso_country",
            "type",
        ]
    )
    airport_df = airport_df[
        (airport_df.iso_country == "US") & ~(airport_df.type == "closed")
    ]
    airport_stats = airport_df[["ident", "elevation_ft", "coordinates"]]

    airport_df.write.parquet(output_data + "airports.parquet")
    airport_stats.write.parquet(output_data + "airports.stats")


def process_temp_data(spark, input_data, output_data):
    """
    loads and processes city data
    input_data: path to data source
    output_data: path to data store
    """

    # data path
    temp_data = os.path.join(
        input_data + "../../data2/GlobalLandTemperaturesByCity.csv"
    )
    temp_df = spark.read.load(temp_data, sep=";", header=True)

    temp_df = check_missing_data(temp_df)


def main():
    input_data = "s3a://udacity-dend/"
    output_data = ""
    spark = createsparksession()
    process_immigration_data(spark, output_data, input_data)
    process_city_data(spark, output_data, input_data)
    process_airport_data(spark, output_data, input_data)
    process_temp_data(spark, output_data, input_data)


if __name__ == "__main__":
    main()
