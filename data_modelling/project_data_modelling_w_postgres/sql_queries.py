# DROP TABLES

songplay_table_drop = "DROP table songplays"
user_table_drop = "DROP table users"
song_table_drop = "DROP table songs"
artist_table_drop = "DROP table artists"
time_table_drop = "DROP table time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays \
     (songplay_id serial constraint songplay_pk primary key,
     start_time timestamp references time (start_time),
     user_id int not null,
     level varchar not null,
     song_id varchar references songs (song_id),
     artist_id varchar references artists (artist_id),
     session_id int not null,
     location varchar,
     user_agent text)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
     (user_id int constraint users_pk primary key, 
    first_name varchar, 
    last_name varchar, 
    gender char(1), 
    level varchar not null)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
    (song_id varchar constraint songs_pk primary key, 
    title varchar, 
    artist_id varchar references artists (artist_id), 
    year int check  (year>=0), 
    duration float)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
    (artist_id varchar constraint artists_pk primary key, 
    name varchar, 
    location varchar, 
    latitude decimal(9,6), 
    longitude decimal(9,6))
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
    (start_time time constraint time_pk primary key,
    hour int not null, 
    day varchar not null, 
    week int not null, 
    month int not null, 
    year int not null, 
    weekday varchar not null)
""")

# INSERT RECORDS
songplay_table_insert = ("""INSERT INTO songplays \
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""INSERT INTO users \
    (user_id, first_name, last_name, gender, level) \
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level
""")

song_table_insert = ("""INSERT INTO songs \
    (song_id, title, artist_id, year, duration) \
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""INSERT INTO artists \
    (artist_id, name, location, latitude, longitude) \
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = ("""INSERT INTO time \
    (start_time, hour, day, week, month, year, weekday) \
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
""")

# FIND SONGS

song_select = ("""SELECT songs.song_id, songs.artist_id \
    FROM songs JOIN artists ON songs.artist_id = artists.artist_id \
    WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]