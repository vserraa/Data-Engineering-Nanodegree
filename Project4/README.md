# Sparkify data warehouse design and ETL pipeline

This is a project from Udacity Data Engineering Nanodegree. In this project, we are responsible for doing a ETL process on some files for a fake startup company called Sparkify.

Initially we have several JSON files in AWS S3 storage that contain data about user activity and metada about songs and artists. The metadata about the songs and artists is from the Million Song Dataset, and the fake user activity is created using an open source event generator.

The ETL process loads the data from S3 buckets and process it using Spark, generating dataframes in a star schema which is better suited for analysis. We have a facts table which consists of user activity records and 4 dimension tables which provide more information about songs, artists, timestamps and users. The dataframes are then uploaded to another bucket in S3 in partitioned parquet files, where they can the be loaded into a traditional data warehouse or further processed using Spark.


### Instructions to run the project

1. Clone the repository into your local workspace
2. Make sure you have python and all the dependecies installed correctly
3. Fill the dl.cfg file with all the necessary information about AWS
4. Run elt.py to run the ETL process. 
5. If you want faster execution with only a subset of the files use the -debug option in the command line. Ex: python etl.py -debug
6. After that, the data will be loaded in the ouput bucket in S3 where it can be used for analysis or further processing.