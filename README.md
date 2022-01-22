# Udacity-Data-Engineering-Nanodegree-Capstone-Project
The aim of this project is to build analytics tables comprising immigration data from the USA and enriched with other data sets such as airport codes, global land temperature and city demographics. 

# Approach 
Build an Amazon S3 hosted data lake and ETL pipeline that loads data from S3, processes it using Spark and loads back to S3 the transformed data either as dimensional tables or fact tables.

# Data Source 

[Immigratin_data]( https://www.trade.gov/national-travel-and-tourism-office)

[Airport data](https://datahub.io/core/airport-codes#data)

[US Cities demography](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)

[temperature](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)

# Conceptual Data Model 

The conceptual data model is that of a star schema comprising a central fact table immigration_fact and auxiliary dimension  tables 

![alt text](https://github.com/TitoLulu/Udacity-Data-Engineering-Nanodegree-Capstone-Project/blob/main/images/conceptual_model.svg?raw=true)

# Run ETL 

To run the ETL enter your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY into capstone.cfg file. Then run the etl.py scrip to trigger spark job that processes and transforms the data into a combination of facts and dimension tables. Check for the output in your s3 data lake to confirm successful load. 

# Clean Up Process

The clean up process involves dropping duplicate rows, reading sas labels files to produce dimension tables for city and country codes, and finally checking for missing data on the temperature dataset. 

# Ideal set up

In a production environment I would inlude a workflow management tool such as airflow to schedule the frequency of DAGs jobs. Assuming it is batch pipeline I would schedule it on a daily to run at 12:00 AM UTC time so as to capture previous days activity data. I would also include redshift warehouse for warehousing allowing analysts to further perform their analysis by either directly querying views or doing that over a BI tool set on top of the warehouse



