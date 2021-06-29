import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from time import time

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['CREDENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Initialize Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process song data, loads from S3, creates tables, save to paquet files on S3

    Args:
        spark (object): Initialized Spark Session
        input_data (str): String of S3 URI for input
        output_data (str): String of S3 URI for output
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"#'song_data/A/A/A/*.json'
    
    # read song data file
    print('Read song data from S3')
    df = spark.read.json(song_data)
    df.createOrReplaceTempView('song_table')
    
    print('Create song table')
    # extract columns to create songs table
    songs_table = spark.sql("""
            SELECT 
            DISTINCT(song_id), title, artist_id, year, duration
            FROM song_table
            WHERE song_id IS NOT NULL
            """)
    
    print('Write song table as parquet to S3')
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").format("parquet").mode("overwrite").save(output_data + 'songs_table.parquet')
    
    print('Create artist table')
    # extract columns to create artists table
    artists_table = spark.sql("""
                    SELECT artist_id,
                    artist_name as name, 
                    artist_location as location,
                    artist_latitude as latitude,
                    artist_longitude as longitude
                    FROM song_table
                    """)
    
    print('Write artist table as parquet to S3')
    # write artists table to parquet files
    artists_table.write.format("parquet").mode("overwrite").save(output_data + 'artists_table.parquet')


def process_log_data(spark, input_data, output_data):
    """Process log data, loads from S3, creates tables, save to paquet files on S3

    Args:
        spark (object): Initialized Spark Session
        input_data (str): String of S3 URI for input
        output_data (str): String of S3 URI for output
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*.json"
    
    print('Read song data from S3')
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    df.createOrReplaceTempView("log_data_table")
    
    print('Create users table')
    # extract columns for users table    
    users_table = spark.sql("""
                    SELECT
                    DISTINCT(userId) as user_id,
                    firstName as first_name,
                    lastName as last_Name,
                    gender,
                    level
                    FROM log_data_table
                    WHERE userId IS NOT NULL
                    """) 
    
    # write users table to parquet files
    print('Write users table as parquet to S3')
    users_table.write.format("parquet").mode("overwrite").save(output_data + 'users_table.parquet')
    """
    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    """
    
    print('Create time table')
    # extract columns to create time table
    time_table = spark.sql("""
                    SELECT timestamp as start_time,
                    hour(timestamp) as hour,
                    day(timestamp) as day,
                    weekofyear(timestamp) as week,
                    month(timestamp) as month,
                    year(timestamp) as year,
                    weekday(timestamp) as weekday
                    FROM(
                    SELECT to_timestamp(ts/1000) as timestamp
                    FROM log_data_table
                    WHERE ts IS NOT NULL)
                    """) 
    
    print('Write time table as parquet to S3')
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').format("parquet").mode("overwrite").save(output_data + 'time_table.parquet')
    
    print('Create songplays table')
    # read in song data to use for songplays table
    songplays_table = spark.sql("""
                SELECT
                monotonically_increasing_id() as songplay_id,
                to_timestamp(ts/1000) as start_time,
                year(to_timestamp(ts/1000)),
                month(to_timestamp(ts/1000)),
                lt.userId as userId,
                lt.level,
                st.song_id,
                st.artist_id,
                lt.sessionId as session_id,
                lt.location,
                lt.userAgent as user_agent
                FROM log_data_table lt
                JOIN song_table st
                ON st.artist_name = lt.artist
                AND lt.song = st.title
                """)
    
    print('Write songplays table as parquet to S3')
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').format("parquet").mode("overwrite").save(output_data + 'songplays_table.parquet')


def main():
    """
    Main function
        - Initialize Spark session
        - Process song data, loads from S3, creates tables, save to paquet files on S3
        - Process log data, loads from S3, creates tables, save to paquet files on S3
    """
    spark = create_spark_session()
    print('Initialized Spark instance')
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/output/"
    
    print('Process song data')
    t1 = time()
    process_song_data(spark, input_data, output_data) 
    print(f'Process song data FINISHED in {time()-t1}s')
    
    print('Process log data')
    t1 = time()
    process_log_data(spark, input_data, output_data)
    print(f'Process log data FINISHED in {time()-t1}s')

if __name__ == "__main__":
    main()
