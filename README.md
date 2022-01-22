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



