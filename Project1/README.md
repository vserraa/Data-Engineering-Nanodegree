# Sparkify data warehouse design and ETL pipeline

This is the first project from Udacity Data Engineering Nanodegree. In this project, we are responsible for creating a data warehouse for a fake startup company called Sparkify.

Initially we have several JSON files that contain data about user activity and metada about songs and artists. The metadata about the songs and artists is from the Million Song Dataset, and the fake user activity is created using an open source event generator.

The data warehouse is designed using a star schema. We have a facts table which consists of user activity records and 4 dimension tables which provide more information about songs, artists, timestamps and users. All the dimension tables are referenced in the main facts table.

The ETL pipeline is written in python. It processes the JSON files and loads the data into a POSTGRES relational database which is better suited for analysis.

### Instructions to run the project

1. Clone the repository into your local workspace
2. Make sure you have python and all the dependecies installed correctly
3. Run create_tables.py to initalize our databsae schema
4. Run elt.py to run the ETL process 
5. After that, our databse is ready for your analytical queries. You can start the test.ipynb notebook which already contains some examples queries and tests and have fun with it!

### Example of analytical queries

1. Get the most popular songs at the moment to analyze trends on the app: 
SELECT songs.song_id, count(*) FROM songplays JOIN songs ON songplays.song_id = songs.song_id WHERE start_time > '2016-6-0 00:00:57.796000' GROUP BY songs.song_id;

2. Get the most active users on the plataform (we could offer those users a premium subscription service): 
SELECT user_id, count(*) as my_count FROM songplays GROUP BY user_id ORDER BY my_count DESC LIMIT 10;