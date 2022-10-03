# About Project Sparkify DataLake 

Music streaming app Sparkify has generated a hug of user base and song database even. These datasets have to be moved from data warehouse to a data lake. Dataset resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in app.

This project builds an ETL pipeline that extracts data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. The result will be used for analytics step to continue finding insights in what songs that the users are listening to.

### Prerequisites

#### Datasets
S3 links for datasets access:
Song data: *s3://udacity-dend/song_data*
Log data: *s3://udacity-dend/log_data*

For song dataset, each file is in JSON format and contains metadata about song and the artist of that song. These files partitioned by the firs thee letters of each song's track ID. Here are examples of filepaths to two files in this dataset:
    *song_data/A/B/C/TRABCEI128F424C983.json*
    *song_data/A/A/B/TRAABJL12903CDCF1A.json*

And how the single song file look like in JSON format:
*{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}*

For log data set, a kind of JSON format too. This dataset contains log files that keep tracked user's activities on app. The log files in this dataset are partitioned by year and month. Here are examples of filepaths to two files in this dataset:
    *log_data/2018/11/2018-11-12-events.json*
    *log_data/2018/11/2018-11-13-events.json*

And then, the detail screenshot of a single JSON log data file
    <log_data-2018-11-12-events.png>

#### Software and Hardware Configuration
The software environent to handle these dataset created on AWS EMR Cluster (Elastic Map Reduced Cluster). It's a AWS cloud base analysis platform with configurations:
    Release: *emr-5.20.0* or later.
    Applications: *Spark 2.4.0 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.0*.
    Instance type: *m3.xlarge*.
    Number of instance: *3*.
    EC2 key pair: Proceed without an EC2 key pair or feel free to use one if you'd like.
Note: keep the remaining default setting and click "Create cluster" on the bottom right.

Create a combined cluster Notebooks to run Spark on EMR cluster.

**Importance Note**: From the moment cloud EMR resource created, AWS will charge running EMR cluster. Pay attention to manage AWS resources to avoid unexpected costs in the **"Managing Resources"**.

### Requirements for Song Play Analysis
Using the song and log datasets, need to create a star schema optimized for queries on song play analysis. The schema like this.
    <Sparkify_DataAnalysis-DB_schema_small.png>

#### Fact Table
1. **songplays** - records in log data associated with song plays i.e. records with page **NextSong**
    *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

#### Dimension Tables
2. **users** - users in the app
    *user_id, first_name, last_name, gender, level*
    
3. **songs** - songs in music database
    *song_id, title, artist_id, year, duration*
    
4. **artists** - artists in music database
    *artist_id, name, location, lattitude, longitude*
    
5. **time** - timestamps of records in songplays broken down into specific units
    *start_time, hour, day, week, month, year, weekday*
    
#### Implement Processes
The project includes 3 files:
    **etl.py** using to reads data from S3, processes that data using Spark, and writes them back to S3
    **dl.cfg** contains AWS authentication credentials. Never expose this file with the AWS keys
    **README.md** provides discussion on your process and decisions
Note: Seperate functions for test included process_log_data.ipynb and process_song_data.ipynb

Initialize a Terminal from Notebook and then run:
    $ **python etl.py*

### Examples verify queries (run bellows on Notebook)
Verify time_table vs parquet file. 
*songs_table.printSchema()*
*songs_table.sort("song_id").show(truncate=False)*
*songs_table = spark.read.parquet(output_data + "songs/songs.parquet/*/*")*
*songs_table.printSchema()*
*songs_table.sort("song_id").show(truncate=False)*
Note: can do the same verification with another tables.

Count amount of fact table record:
    *songplays_count* = *spark.sql*("""
                                **SELECT** COUNT(*) 
                                **FROM** songplays_table
                                """)
    *print(songplays_count)*

Duplicate users verification
    *users_duplicated* = *spark.sql*("""
                                **SELECT** user_id, COUNT(*) as count 
                                **FROM** users 
                                **GROUP BY** user_id 
                                **ORDER BY** count DESC
                                """)
    *print(users_duplicated)*

NULL songplays verification
    *songplays_null* = *spark.sql*("""
                                **SELECT** COUNT(*) 
                                **FROM** songplay 
                                **WHERE** songplay_id = NULL
                                """)
    *print(songplays_null)*
