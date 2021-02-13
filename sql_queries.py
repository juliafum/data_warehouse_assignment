import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSON_PATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES


staging_events_table_create = ("""create table if not exists staging_events (
artist varchar(500),
auth varchar(20),
first_name varchar(50),
gender varchar(1),
item_in_session int,
last_name varchar(50),
length numeric(11,5),
level varchar(50),
location text,
method varchar(5),
page varchar(20),
registration float,
session_id int,
song text,
status int,
ts bigint,
user_agent varchar(500),
user_id int  
)
""")

staging_songs_table_create = (""" create table if not exists staging_songs (
num_songs int,
artist_id varchar(25),
artist_latitude numeric(10,5),
artist_longitude numeric(10,5),
artist_location text,
artist_name varchar(200),
song_id varchar(25),
title varchar(200),
duration numeric(10,5),
year int
)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
songplayid int IDENTITY(0,1) PRIMARY KEY NOT NULL,
start_time datetime NOT NULL,
user_id int NOT NULL ,
level varchar(50),
song_id varchar(25),
artist_id varchar(25),
session_id int,
location varchar(256),
user_agent varchar(500)
)
""")

user_table_create = ("""create table if not exists users  (
user_id int  PRIMARY KEY NOT NULL,
user_first_name varchar(50),
user_last_name varchar(50),
gender varchar(1),
level varchar(50)
)        
""")

song_table_create = ("""create table if not exists songs (
song_id nvarchar(25) PRIMARY KEY NOT NULL,
title text,
artist_id varchar(25),
year int,
duration numeric(10,5)
)       
""")

artist_table_create = ("""create table if not exists artists (
artist_id varchar(25) PRIMARY KEY NOT NULL,
artist_name varchar(500),
artist_location text,
artist_latitude numeric(10,5),
artist_longitude numeric(10,5) 
)        
""")

time_table_create = ("""create table if not exists time (
start_time  datetime PRIMARY KEY NOT NULL,
hour int,
week int,
month int,
year int,
weekday int
)       
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events FROM {}
                          credentials 'aws_iam_role={}'
                          region 'us-west-2'
                          format as json {};
                          """).format(LOG_DATA, IAM_ROLE, LOG_JSON_PATH)

staging_songs_copy = ("""copy staging_songs FROM {}
                        credentials 'aws_iam_role={}'
                        region 'us-west-2'
                        json 'auto';
                        """).format(SONG_DATA, IAM_ROLE)


# FINAL TABLES
songplay_table_insert = ("""insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            select distinct timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time, user_id, level, song_id,
                            artist_id, session_id, location, user_agent
                            from staging_events as e
                            join staging_songs as s
                            on e.artist = s.artist_name and e.song = s.title        
                            where page = 'NextSong'
                             
""")

user_table_insert = ("""insert into users (user_id,user_first_name,user_last_name,gender,level)
                        select distinct user_id ,first_name,last_name,gender,level
                        from staging_events WHERE user_id is not null
""")

song_table_insert = ("""insert into songs (song_id,title,artist_id,year, duration)
                        select distinct song_id, title, artist_id, year,duration
                        from staging_songs
""")

artist_table_insert = ("""insert into artists (artist_id, artist_name,artist_location,artist_latitude,artist_longitude)
                           select distinct artist_id,artist_name,artist_location,artist_latitude ,artist_longitude
                           FROM staging_songs where artist_id is not null
""")

time_table_insert = ("""INSERT INTO time (start_time,hour,week, month,year,weekday)
                        select timestamp 'epoch' + ts/1000 * interval '1 second' as start_time,
                        extract (hour from timestamp 'epoch' + ts/1000 * interval '1 second') as hour,
                        extract (week from timestamp 'epoch' + ts/1000 * interval '1 second') as week,
                        extract (month from timestamp 'epoch' + ts/1000 * interval '1 second') as month,
                        extract (year from timestamp 'epoch' + ts/1000 * interval '1 second') as year,
                        extract (weekday from timestamp 'epoch' + ts/1000 * interval '1 second') as weekday
                        from staging_events 
                                    
                     
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
