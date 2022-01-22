# Udacity-Data-Engineering-Nanodegree-Capstone-Project
The aim of this project is to build analytics tables comprising immigration data from the USA and enriched with other data sets such as airport codes, global land temperature and city demographics. Analysts can perfom analysis like checking the frequency of immigration from a particular country partitioned by month and year and country.

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

# inceasing data by 100x
If data was increased by 100x Spark's powerful processing engine would be able to handle it. We could also process the data in partitions so as to work within memory limits. Also, in place of running the Spark job on single user machined we would host Spark in AWS EMR which allows running peta-byte scale data analytics faster

# The data populates a dashboard that must be updated on a daily basis by 7am every day

 As the data immigration is updated on a monthly basis, I would schedule the monthly DAG to run before 7 AM, so give or take 2 hours before to append new data in the tables so that the downstream dependent task does not fail or load empty

# The database needed to be accessed by 100+ people

Implement access management by adding users to groups with different data needs,then grant read permissions to tables frequently used by the different groups. Hence based on group/role all I would need to do when a new analyst/employee joins the company is add them to any of the predefined group and they inherit all access rights relating to the group.

# Reference
[ref #1](https://sparkbyexamples.com/pyspark/pyspark-split-dataframe-column-into-multiple-columns/):**PySpark split() Column into Multiple Columns**

[ref #2](https://changhsinlee.com/pyspark-udf/):**How to Turn Python Functions into PySpark Functions (UDF)**

[ref #3](https://realpython.com/python-zip-function/):**ZIP Function**



