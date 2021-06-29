# Project: Data Lake

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. For this we built an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to. With this project project combines method and techniques from Spark, Data Lakes and [Amazon Web Service (AWS)](https://aws.amazon.com/de/).

## Project Datasets
For this project we use data from two datasets, that are stored in s3 buckets on AWS:
Song Data Path --> s3://udacity-dend/song_data
Log Data Path --> s3://udacity-dend/log_data

Song Dataset
The first dataset is a subset of real data from the Million Song Dataset(https://labrosa.ee.columbia.edu/millionsong/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example:

song_data/A/B/C/TRABCEI128F424C983.json song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

Log Dataset

The second dataset consists of log files in JSON format. The log files in the dataset with are partitioned by year and month. For example:

log_data/2018/11/2018-11-12-events.json log_data/2018/11/2018-11-13-events.json

And below is an example of what a single log file, 2018-11-13-events.json, looks like.

{"artist":"Pavement", "auth":"Logged In", "firstName":"Sylvie", "gender", "F", "itemInSession":0, "lastName":"Cruz", "length":99.16036, "level":"free", "location":"Klamath Falls, OR", "method":"PUT", "page":"NextSong", "registration":"1.541078e+12", "sessionId":345, "song":"Mercy:The Laundromat", "status":200, "ts":1541990258796, "userAgent":"Mozilla/5.0(Macintosh; Intel Mac OS X 10_9_4...)", "userId":10}

## Tables
For the etl we utilize one fact table (songplays) and four dimension tables (users, songs, artists and time).

Fact Table

songplays - records in event data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables

users - users in the app user_id, first_name, last_name, gender, level

songs - songs in music database song_id, title, artist_id, year, duration

artists - artists in music database artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday

## Folder Structure
- etl.py reads data from S3, processes that data using Spark and writes them back to S3
- dl.cfg contains AWS Credentials
- README.md README
- etl.ipynb Jupyter Notebook for development
- data
    - log-data.zip zipped folder with jsons of log-data
    - song-data.zip zipped folder with jsons of song-data
    
## Run
0. Install [Spark & Pyspark](https://spark.apache.org/docs/latest/api/python/index.html) if not installed ```pip install pyspark```
1. Implement AWS credentials (access key and secret access key) in dl.cfg
2. (Optional) Load log data and song data from data folder in own s3 bucket.
    Might be necessary if you do not use the AWS acount given by the udacity course
3. Run ```python etl.py```

## ETL Pipeline
1. Loading credentials from dl.cfg
2. Creating a Spark instance
3. Loading data from s3 buckets
4. Process data using Spark to create described tables
5. Load tables back to new s3 output bucket

