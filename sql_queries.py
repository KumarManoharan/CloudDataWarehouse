import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop  = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop       = "DROP TABLE IF EXISTS songplay"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS songs"
artist_table_drop         = "DROP TABLE IF EXISTS artist"
time_table_drop           = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS stg_events
(
artist          VARCHAR,
auth            VARCHAR, 
firstName       VARCHAR,
gender          VARCHAR,   
itemInSession   INTEGER,
lastName        VARCHAR,
length          FLOAT,
level           VARCHAR, 
location        VARCHAR,
method          VARCHAR,
page            VARCHAR,
registration    BIGINT,
sessionId       INTEGER,
song            VARCHAR,
status          INTEGER,
ts              TIMESTAMP,
userAgent       VARCHAR,
userId          INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS stg_songs
(
song_id            VARCHAR,
num_songs          INTEGER,
title              VARCHAR,
artist_name        VARCHAR,
artist_latitude    FLOAT,
year               INTEGER,
duration           FLOAT,
artist_id          VARCHAR,
artist_longitude   FLOAT,
artist_location    VARCHAR
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay
(
songplay_id          INTEGER IDENTITY(0,1) PRIMARY KEY SORTKEY,
start_time           TIMESTAMP,
user_id              INTEGER,
level                VARCHAR,
song_id              VARCHAR,
artist_id            VARCHAR,
session_id           INTEGER,
location             VARCHAR,
user_agent           VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id INTEGER PRIMARY KEY DISTKEY,
first_name      VARCHAR,
last_name       VARCHAR,
gender          VARCHAR,
level           VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
song_id     VARCHAR PRIMARY KEY,
title       VARCHAR,
artist_id   VARCHAR DISTKEY,
year        INTEGER,
duration    FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist
(
artist_id          VARCHAR PRIMARY KEY DISTKEY,
name               VARCHAR,
location           VARCHAR,
latitude           FLOAT,
longitude          FLOAT
);

""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time    TIMESTAMP PRIMARY KEY SORTKEY DISTKEY,
hour          INTEGER,
day           INTEGER,
week          INTEGER,
month         INTEGER,
year          INTEGER,
weekday       INTEGER
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY stg_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), config.get("S3", "LOG_JSONPATH")) 

staging_songs_copy = ("""
    COPY stg_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                se.userId as user_id,
                se.level as level,
                ss.song_id as song_id,
                ss.artist_id as artist_id,
                se.sessionId as session_id,
                se.location as location,
                se.userAgent as user_agent
FROM stg_events se
JOIN stg_songs ss ON se.song = ss.title AND se.artist = ss.artist_name;
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId as user_id,
                firstName as first_name,
                lastName as last_name,
                gender as gender,
                level as level
FROM stg_events
where userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id as song_id,
                title as title,
                artist_id as artist_id,
                year as year,
                duration as duration
FROM stg_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artist(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id as artist_id,
                artist_name as name,
                artist_location as location,
                artist_latitude as latitude,
                artist_longitude as longitude
FROM stg_songs
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
FROM stg_events
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
