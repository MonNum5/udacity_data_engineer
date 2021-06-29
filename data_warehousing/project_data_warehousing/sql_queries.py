import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS ongplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession integer,
        lastName varchar,
        length float,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration bigint,
        sessionId integer,
        song varchar,
        status integer,
        ts timestamp,
        userAgent varchar,
        userId integer
    )
    
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        numsongs integer,
        artist_id varchar,
        artist_latitude float,
        artist_longitude float,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration float,
        year int
    )
""")


songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
     (
         songplay_id integer identity (1,1) primary key,
         start_time timestamp,
         user_id int not null,
         level varchar not null,
         song_id varchar,
         artist_id varchar,
         session_id int not null,
         location varchar,
         user_agent text
     )
     DISTSTYLE KEY 
     DISTKEY (start_time)
     SORTKEY (start_time)
    
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id int primary key, 
        first_name varchar, 
        last_name varchar, 
        gender char(1), 
        level varchar not null
    )
    SORTKEY (user_id)
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id varchar primary key, 
        title varchar, 
        artist_id varchar, 
        year int, 
        duration float
    )
    SORTKEY (song_id)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id varchar primary key, 
        name varchar, 
        location varchar, 
        latitude decimal(9,6), 
        longitude decimal(9,6)
    )
    SORTKEY (artist_id)
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time timestamp primary key,
        hour int not null, 
        day varchar not null, 
        week int not null, 
        month int not null, 
        year int not null, 
        weekday varchar not null
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role {}
    FORMAT AS json {}
    timeformat as 'epochmillisecs'
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    iam_role {}
    FORMAT AS json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays \
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
    SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                se.userId as user_id,
                se.level as level,
                ss.song_id as song_id,
                ss.artist_id as artist_id,
                se.sessionId as session_id,
                se.location as location,
                se.userAgent as user_agent
    FROM staging_events se
    JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name;
""")

user_table_insert = ("""
    INSERT INTO users \
    (user_id, first_name, last_name, gender, level) \
    SELECT DISTINCT userId as user_id,
                firstName as first_name,
                lastName as last_name,
                gender as gender,
                level as level
    FROM staging_events
    WHERE userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs \
    (song_id, title, artist_id, year, duration) \
    SELECT DISTINCT song_id, title, artist_id, year as year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id as artist_id,
                    artist_name as name,
                    artist_location as location,
                    artist_latitude as latitude,
                    artist_longitude as longitude
    FROM staging_songs
    where artist_id IS NOT NULL;
""")


time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT distinct ts,
                    EXTRACT(hour from ts),
                    EXTRACT(day from ts),
                    EXTRACT(week from ts),
                    EXTRACT(month from ts),
                    EXTRACT(year from ts),
                    EXTRACT(weekday from ts)
    FROM staging_events
    WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
