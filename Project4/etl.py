import configparser
from datetime import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType, DateType, LongType, DoubleType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

song_schema = StructType([
    StructField("num_songs", IntegerType(), True),
    StructField("artist_id", StringType(), True),
    StructField("artist_latitude", DecimalType(), True),
    StructField("artist_longitude", DecimalType(), True),
    StructField("artist_location", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("song_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("duration", DecimalType(), True),
    StructField("year", IntegerType(), True)
])
    
log_schema = StructType([
    StructField("artist", StringType(), True),
    StructField("auth", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("lastName", StringType(), True),
    StructField("length", DecimalType(), True),
    StructField("level", StringType(), True),
    StructField("location", StringType(), True),
    StructField("method", StringType(), True),
    StructField("page", StringType(), True),
    StructField("registration", StringType(), True),
    StructField("sessionId", StringType(), True),
    StructField("song", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("ts", LongType(), True),
    StructField("userAgent", StringType(), True),
    StructField("userId", StringType(), True)
])

def create_spark_session():
    '''
        Description: Creates a spark session
        
        return: The spark session that was created
    '''
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def read_song_data(spark, input_data, my_schema, debug = False):
    '''
        Parameters:
        .spark -> A spark session
        .input_data -> Path to the input bucket in S3
        .my_schema -> The schema used to read the data from S3
        .debug -> A optional parameter to read only a subset of data for debugging porpouses
        
        Description: Loas the data from S3 into a spark dataframe
        
        return: The spark dataframe
    '''
    if debug:
        path = os.path.join(input_data, "song_data/A/A/A/*.json")
    else:
        path = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    return spark.read.json(path, schema=my_schema)


def read_log_data(spark, input_data, my_schema, debug = False):
    '''
        Parameters:
        .spark -> A spark session
        .input_data -> Path to the input bucket in S3
        .my_schema -> The schema used to read the data from S3
        .debug -> A optional parameter to read only a subset of data for debugging porpouses
        
        Description: Loas the data from S3 into a spark dataframe
        
        return: The spark dataframe
    '''
    
    if debug:
        path = os.path.join(input_data, 'log_data/2018/11/*.json')
    else:
        path = os.path.join(input_data, 'log_data/*/*/*.json')
    
    return spark.read.json(path, schema=my_schema)


def process_song_data(spark, song_data, output_data):
    '''
        Parameters:
        .spark -> A spark session
        .song_data -> A spark dataframe with data from S3 to be processed
        .output_data -> Path to a output bucket in S3 where the results will be stored
        
        Description: Processes the song_data dataframe and loads results into S3 as parquet files
    '''
    
    # extract columns to create songs table
    songs_columns = ["song_id", "title", "artist_id", "year", "duration"]
    songs_df = song_data.select(*songs_columns)
    
    #partitioning songs dataframe by year and then artist as requested
    songs_df = songs_df.repartition("year", "artist_id")
    
    songs_output_path = os.path.join(output_data, "songs/")
    
    songs_df = songs_df.dropDuplicates()
    
    songs_df.write.parquet(songs_output_path, mode='overwrite')

    # extract columns to create artists table
    artists_columns = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_df = song_data.select(*artists_columns) 
    
    # write artists table to parquet files
    artists_output_path = os.path.join(output_data, "artists/")
    
    artists_df = artists_df.dropDuplicates()
    
    artists_df.write.parquet(artists_output_path, mode='overwrite')
    
def process_log_data(spark, song_df, log_df, output_data):  
    '''
        Parameters:
        .spark -> A spark session
        .song_df -> A spark dataframe with data from S3 to be processed
        .log_df -> A spark dataframe with data from S3 to be processed 
        .output_data -> Path to a output bucket in S3 where the results will be stored
        
        Description: Processes the song_df and log_df dataframes and loads results into S3 as parquet files
    '''
        
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')

    user_exprs = ["userId AS user_id", "firstName AS first_name", "lastName AS last_name",
                  "gender AS gender", "level AS level"]
    
    # extract columns for users table    
    users_df = log_df.selectExpr(*user_exprs)
    
    # write users table to parquet files
    users_out_path = os.path.join(output_data, 'users/')
    
    users_df = users_df.dropDuplicates()
    
    users_df.write.parquet(users_out_path, mode='overwrite')
    
    # create timestamp column from original timestamp column
    ts_df = log_df.withColumn('timestamp', (log_df['ts']/1000).cast(TimestampType()))
    
    # create datetime column from original timestamp column
    date_df = ts_df.withColumn('datetime', ts_df['timestamp'].cast(DateType()))
            
    from pyspark.sql.functions import year, month, dayofmonth, dayofweek, weekofyear, dayofyear, hour
    # extract columns to create time table
    time_df = date_df.select("timestamp",
                              hour("timestamp").alias("hour"),
                              dayofmonth("timestamp").alias("day"),
                              weekofyear("timestamp").alias("week"),
                              month("timestamp").alias("month"),
                              year("timestamp").alias("year"),
                              dayofweek("timestamp").alias("day_of_week")
                            )
      
    time_df = time_df.repartition("year", "month")
    
    time_df = time_df.dropDuplicates()
    
    time_df_output_path = os.path.join(output_data, 'time/')
    
    time_df.write.parquet(time_df_output_path, mode='overwrite')
    
    cond = [date_df.artist == song_df.artist_name, date_df.song == song_df.title]
    joined_df = date_df.join(song_df, cond, how='left').drop(song_df.year)
    
    songplays_df = joined_df.selectExpr("timestamp",
                                        "userId AS user_id",
                                        "level",
                                        "song_id",
                                        "artist_id",
                                        "sessionId AS session_id",
                                        "location",
                                        "userAgent AS user_agent")
                                    
    songplays_df = songplays_df.withColumn("year", year("timestamp")).withColumn("month", month("timestamp"))   
     
    songplays_df = songplays_df.repartition("year", "month")
    
    songplays_df = songplays_df.dropDuplicates()
    
    songplays_output_path = os.path.join(output_data, 'songplays/')
    
    songplays_df.write.parquet(songplays_output_path, mode='overwrite')

def main(argv):
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://vssm-udacity-bucket/"
    
    if len(argv) == 1 and argv[0] == '-debug':
        debug = True
    else:
        debug = False
        
    spark = create_spark_session()
    
    song_df = read_song_data(spark, input_data, song_schema, debug)
    log_df = read_log_data(spark, input_data, log_schema, debug)
    
    process_song_data(spark, song_df, output_data)    
    process_log_data(spark, song_df, log_df, output_data)


if __name__ == "__main__":
    main(sys.argv[1:])
