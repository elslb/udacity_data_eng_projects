import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""

CREATE TABLE IF NOT EXISTS staging_events (
                                artist VARCHAR,
                                auth VARCHAR,
                                first_name VARCHAR,
                                gender VARCHAR,
                                item_in_session INTEGER,
                                last_name VARCHAR,
                                length DECIMAL,
                                level VARCHAR,
                                artist_location VARCHAR,
                                method VARCHAR,
                                page VARCHAR,
                                registration VARCHAR,
                                session_id INTEGER,
                                song VARCHAR,
                                status INTEGER,
                                ts TIMESTAMP,
                                user_agent VARCHAR,
                                user_id INTEGER
                                
);
""")

staging_songs_table_create = ("""

CREATE TABLE IF NOT EXISTS staging_songs (
                                num_songs INTEGER,
                                artist_id VARCHAR SORTKEY DISTKEY,
                                artist_latitude DECIMAL,
                                artist_longitude DECIMAL,
                                artist_location VARCHAR,
                                artist_name VARCHAR,
                                song_id VARCHAR,
                                title VARCHAR,
                                duration DECIMAL,
                                year INTEGER
);
""")

songplay_table_create = ("""

CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id INTEGER IDENTITY(0, 1) SORTKEY PRIMARY KEY,
                                start_time TIMESTAMP,
                                user_id INTEGER DISTKEY,
                                level VARCHAR,
                                song_id VARCHAR,
                                artist_id VARCHAR,
                                session_id INTEGER,
                                artist_location VARCHAR,
                                user_agent VARCHAR
);
""")

user_table_create = ("""

CREATE TABLE IF NOT EXISTS users (
                                user_id INTEGER SORTKEY PRIMARY KEY,
                                first_name VARCHAR,
                                last_name VARCHAR,
                                gender VARCHAR,
                                level VARCHAR
);
""")

song_table_create = ("""

CREATE TABLE IF NOT EXISTS songs (
                                song_id VARCHAR SORTKEY PRIMARY KEY,
                                title VARCHAR,
                                artist_id VARCHAR,
                                year INTEGER,
                                duration DECIMAL
);
""")

artist_table_create = ("""

CREATE TABLE IF NOT EXISTS artists (
                                artist_id VARCHAR SORTKEY PRIMARY KEY,
                                artist_name VARCHAR,
                                artist_location VARCHAR,
                                artist_latitude DECIMAL,
                                artist_longitude DECIMAL
);
""")

time_table_create = ("""

CREATE TABLE IF NOT EXISTS times (
                                start_time TIMESTAMP SORTKEY PRIMARY KEY,
                                hour SMALLINT,
                                day SMALLINT,
                                week SMALLINT,
                                month SMALLINT,
                                year SMALLINT,
                                weekday SMALLINT
);
""")

# STAGING TABLES

staging_events_copy = ("""
                    COPY staging_events
                    FROM {}
                    CREDENTIALS 'aws_iam_role={}'
                    REGION 'us-west-2'
                    FORMAT as JSON {}
                    TIMEFORMAT as 'epochmillisecs';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
                    COPY staging_songs
                    FROM {}
                    CREDENTIALS 'aws_iam_role={}'
                    REGION 'us-west-2'
                    FORMAT as JSON 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""

INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, artist_location, user_agent)
SELECT DISTINCT se.ts,
                se.user_id,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.session_id,
                se.artist_location,
                se.user_agent
FROM staging_events se
JOIN staging_songs ss ON (se.song = ss.title AND se.artist = ss.artist_name)
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""

INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id,
                first_name,
                last_name,
                gender,
                level
FROM staging_events
WHERE user_id IS NOT NULL AND page = 'NextSong';
""")

song_table_insert = ("""

INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
                title,
                artist_id,
                year,
                duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""

INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
SELECT DISTINCT artist_id,
                artist_name,
                artist_location,
                artist_latitude,
                artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;                
""")

time_table_insert = ("""

INSERT INTO times (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts,
                EXTRACT (hour FROM ts),
                EXTRACT (day FROM ts),
                EXTRACT (week FROM ts),
                EXTRACT (month FROM ts),
                EXTRACT (year FROM ts),
                EXTRACT (dayofweek FROM ts)
FROM staging_events
WHERE ts IS NOT NULL AND page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, song_table_create, artist_table_create, time_table_create, user_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
