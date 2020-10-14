# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"
temp_time_table_drop = "DROP TABLE IF EXISTS temptimes;"
temp_user_table_dorp = "DROP TABLE IF EXISTS tempusers;"
temp_songplay_table_drop = "DROP TABLE IF EXISTS tempsongplays;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id int PRIMARY KEY, start_time timestamp NOT NULL, 
                            user_id int NOT NULL, level char(4) NOT NULL, song_id varchar(100), artist_id varchar(100), session_id int NOT NULL, 
                            location varchar(100), user_agent text NOT NULL);
                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id integer PRIMARY KEY, first_name varchar(50) NOT NULL, 
                        last_name varchar(50) NOT NULL, gender char(1), level char(4) NOT NULL);
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar(100), title varchar(100) NOT NULL, 
                         artist_id varchar(100), year int, duration decimal,
                         PRIMARY KEY (song_id, artist_id));
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar(100) PRIMARY KEY, name varchar(100) NOT NULL, 
                          location varchar(100), lattitude decimal, longitude decimal);
                      """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS times (start_time timestamp PRIMARY KEY, hour int NOT NULL, 
                        day int NOT NULL, week int NOT NULL, month int NOT NULL, 
                        year int NOT NULL , weekday int NOT NULL);
                    """)

# CREATE TEMPORARY TABLES
temp_time_table_create = ("""CREATE TABLE IF NOT EXISTS temptimes (start_time timestamp NOT NULL, hour int NOT NULL, 
                            day int NOT NULL, week int NOT NULL, month int NOT NULL, 
                            year int NOT NULL , weekday int NOT NULL);
                        """)
temp_user_table_create = ("""CREATE TABLE IF NOT EXISTS tempusers (user_id integer NOT NULL, first_name varchar(50) NOT NULL,
                             last_name varchar(50) NOT NULL, gender char(1), level char(4) NOT NULL);
                          """)
temp_songplay_table_create = ("""CREATE TABLE IF NOT EXISTS tempsongplays (songplay_id int NOT NULL, start_time timestamp NOT NULL, 
                            user_id int NOT NULL, level char(4) NOT NULL, song_id varchar(100), artist_id varchar(100), session_id int NOT NULL, 
                            location varchar(100), user_agent text NOT NULL);
                        """)


# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays SELECT songplay_id, start_time, user_id, level, song_id, 
                            artist_id, session_id, location, user_agent 
                            FROM tempsongplays ON CONFLICT DO NOTHING;
                        """)

songplay_table_copy = ("""COPY tempsongplays (songplay_id, start_time, user_id, level, song_id, 
                        artist_id, session_id, location, user_agent) 
                        FROM '/home/workspace/data/songplay_log_file.csv' 
                        DELIMITER ','
                        CSV HEADER;
                      """)

user_table_insert = ("""INSERT INTO users SELECT user_id, first_name, last_name, gender, level
                        FROM tempusers ON CONFLICT (user_id) DO UPDATE SET level=excluded.level;
                    """)

user_table_copy = ("""COPY tempusers (user_id, first_name, last_name, gender, level) 
                    FROM '/home/workspace/data/user_log_file.csv' 
                    DELIMITER ','
                    CSV HEADER;
                  """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
                     """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude)
                          VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
                       """)

time_table_insert = ("""INSERT INTO times SELECT start_time, hour, day, week, month, year, weekday 
                        FROM temptimes ON CONFLICT DO NOTHING;
                    """)

time_table_copy = ("""COPY temptimes (start_time, hour, day, week, month, year, weekday) 
                    FROM '/home/workspace/data/time_log_file.csv' 
                    DELIMITER ','
                    CSV HEADER;
                  """)

# FIND SONGS

song_select = ("""SELECT songs.song_id, artists.artist_id FROM songs JOIN artists on songs.artist_id=artists.artist_id
                  WHERE (songs.title=%s AND artists.name=%s AND songs.duration=%s);
               """)

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create,
                        time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, 
                      time_table_drop]
