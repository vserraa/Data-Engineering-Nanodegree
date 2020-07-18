# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id int PRIMARY KEY,
                                                                  start_time timestamp NOT NULL,
                                                                  user_id text NOT NULL,
                                                                  level text,
                                                                  song_id text,
                                                                  artist_id text,
                                                                  session_id text NOT NULL,
                                                                  location text,
                                                                  user_agent text)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id text PRIMARY KEY,
                                                          first_name text,
                                                          last_name text,
                                                          gender text,
                                                          level text)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id text PRIMARY KEY, 
                                                          title text,
                                                          artist_id text,
                                                          year int,
                                                          duration numeric)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id text PRIMARY KEY,
                                                              name text,
                                                              location text,
                                                              latitude numeric,
                                                              longitude numeric)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTs time (start_time timestamp PRIMARY KEY,
                                                         hour int,
                                                         day int,
                                                         week int,
                                                         month int,
                                                         year int,
                                                         weekday int)
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            values (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        values (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
                        values (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          values (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        values (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING""")

# FIND SONGS

song_select = ("""SELECT song_id, artists.artist_id FROM (songs JOIN artists on songs.artist_id = artists.artist_id) WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s;""")

get_songplay_id_start = ("""SELECT max(songplay_id) FROM songplays""")

# QUERY LISTS 

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# SOME POSSIBLE ANALYTICAL QUERIES

#popular songs at the moment
popular_songs = ("""SELECT songs.song_id, count(*) FROM songplays JOIN songs ON songplays.song_id = songs.song_id WHERE start_time > '2017-11-29 00:00:57.796000' GROUP BY songs.song_id;""")

#top users --> we could recommend a premium subscription for those users
top_users = ("""SELECT user_id, count(*) as my_count FROM songplays GROUP BY user_id ORDER BY my_count DESC LIMIT 10;""")
