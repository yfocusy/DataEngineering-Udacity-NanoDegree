import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql import functions as F
from pyspark.sql import types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
#     song_data = input_data + "song_data/A/A/A/*.json"
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title","artist_id","year","duration"]).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + 'songs_table', partitionBy=["year","artist_id"], mode='overwrite')

    # extract columns to create artists table
    artists_table = df.selectExpr(["artist_id", 
                           "artist_name as name", 
                           "artist_location as location",
                           "artist_latitude as latitude",
                           "artist_longitude as longitude"]).dropDuplicates()  
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists_table', mode = 'overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
#     log_data = input_data + 'log_data/2018/*/*.json'
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')

    # extract columns for users table    
    users_table = df.selectExpr(["userId as user_id", 
                                "firstName as first_name", 
                                "lastName as last_name", 
                                "gender", 
                                "level"]).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users_table', mode = 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp(x/1000),T.TimestampType())
    df = df.withColumn('timestamp',get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    df = df.withColumn('date_time',F.to_date(df.timestamp))
    
    # extract columns to create time table
    time_table = df.selectExpr(['timestamp as start_time', 
                             'hour(timestamp) as hour',
                             'dayofmonth(timestamp) as day',
                             'weekofyear(timestamp) as week',
                             'month(timestamp) as month',
                             'year(timestamp) as year',
                             'dayofweek(timestamp) as weekday'                    
                            ]).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'times_table', partitionBy=['year','month'],mode='overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs_table')
    

    # extract columns from joined song and log datasets to create songplays table 
    songplays_column_name = ['timestamp as start_time',
                             'userId as user_id', 
                             'level',
                             'song_id',
                             'artist_id', 
                             'sessionId as session_id',
                             'location','userAgent as user_agent', 
                             'year(timestamp) as year', 
                             'month(timestamp) as month']
    
    songplays_table = df.join(song_df, 
                              [df.song==song_df.title, df.length ==song_df.duration],
                               how='left') \
                         .selectExpr(songplays_column_name) \
                         .withColumn('songplay_id', F.monotonically_increasing_id())


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays_table', partitionBy=['year', 'month'], mode='overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://outputdata/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
