import configparser
from datetime import datetime
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file - this path for test only
    # song_data = input_data + "song_data/*/*/*/*.json" # for cloud submit version
    song_data = input_data +"songdata/*/*/*/*.json" # for local test only
    
    # read song data file
    df = spark.read.json(song_data)
        
    # extract columns to create songs table
    """
    Create songs table - songs in music database: 
    song_id, title, artist_id, year, duration
    """
    songs_table = df.select("song_id","title","artist_id","year","duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs/songs.parquet"), "overwrite")

    # extract columns to create artists table
    """
    Create artists table - artists in music database
    artist_id, name, location, lattitude, longitude
    """
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists/artists.parquet"), "overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    # log_data = input_data + "log_data/*.json" # for cloud submit version
    log_data = input_data + "logdata/*.json" # for submit version
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')


    # extract columns for users table    
    users_table = df.select("userId","firstName","lastName","gender","level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users/users.parquet"), "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table
    """
    Create time table - timestamps of records in songplays broken down into specific units
    start_time, hour, day, week, month, year, weekday
    """    
    time_table = df.select(col('timestamp').alias('start_time'),
                       hour('timestamp').alias('hour'),
                       dayofmonth('timestamp').alias('day'),
                       weekofyear('timestamp').alias('week'),
                       month('timestamp').alias('month'),
                       year('timestamp').alias('year'),
                       date_format('timestamp','E').alias('weekday')
                    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time/time.parquet"), "overwrite")
    
    # read in song data to use for songplays table
    #songplays_log = spark.read.json(log_data) # get full log of songplay
    df = df.withColumn('songplay_id', F.monotonically_increasing_id())
    df.createOrReplaceTempView("log_data_table")
    #song_df.printSchema()
    df.sort("song").show(truncate=False) # Verify log_data_table
    
    song_data = input_data +"songdata/*/*/*/*.json" # for local test only
    song_df = spark.read.json(song_data)
    song_df.createOrReplaceTempView("song_data_table") # get filtered of songplay with page='NextSong'
    song_df.printSchema()
    song_df.sort("title").show(truncate=False) # Verify song_data_table

    # extract columns from joined song and log datasets to create songplays table 
    """
    Create songplays table - records in log data associated with song plays i.e. records with page NextSong
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    """    
    songplays_table = spark.sql("""
    SELECT
        ldt.ts as start_time,
        ldt.userId AS user_id,
        ldt.level as level,
        sdt.song_id as song_id,
        sdt.artist_id as artist_id,
        ldt.sessionId as session_id,
        ldt.location as location,
        ldt.userAgent as user_agent,
        year(ldt.timestamp) as year,
        month(ldt.timestamp) as month,
        ldt.songplay_id as songplay_id
    FROM log_data_table ldt
    JOIN song_data_table sdt 
    ON ldt.song = sdt.title AND ldt.artist = sdt.artist_name
    """)

    # write songplays table to parquet files partitioned by year and month
    # songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "songplays/songplays.parquet"), "overwrite")
    songplays_table.write.mode("overwrite").parquet(os.path.join(output_data, 'songplays'), partitionBy=['year', 'month'])
    
    # Verify songplays_table parquet file
    print("This is songplays_table:")
    songplays_table.printSchema()
    songplays_table.sort("start_time").show(truncate=False)

    # print("This is songplays_parquet:")
    # songplays_parquet = spark.read.parquet(output_data + "songplays")
    # songplays_parquet = spark.read.parquet("./output/songplays/songplays.parquet")
    # songplays_parquet.printSchema()
    # songplays_parquet.sort("start_time").show(truncate=False)


def main():
    spark = create_spark_session()
    ## Define in/out data for cloud version
    # input_data = "s3a://udacity-dend/"
    # output_data = "s3://thanhdv-spark-datalake-emr-project4-03/emr-lab-data/"
    
    ## Define in/out data for local test version
    input_data = "./input/"
    output_data = "./output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
