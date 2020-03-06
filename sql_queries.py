import configparser


def get_copy_json_query(table_name, s3_url, iam_role, json_paths='auto'): 
    return (
        f"COPY {table_name} FROM '{s3_url}'"
        f" IAM_ROLE '{iam_role}'"
        f" FORMAT AS JSON '{json_paths}'"
    )


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN                    = config.get('IAM_ROLE', 'ARN')
S3_LOG_DATA            = config.get('S3', 'LOG_DATA')
S3_LOG_JSONPATH        = config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA           = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = 'DROP TABLE IF EXISTS event_staging;'
staging_songs_table_drop = 'DROP TABLE IF EXISTS song_staging;'
songplay_table_drop = 'DROP TABLE IF EXISTS songplays CASCADE;'
user_table_drop = 'DROP TABLE IF EXISTS users;'
song_table_drop = 'DROP TABLE IF EXISTS songs;'
artist_table_drop = 'DROP TABLE IF EXISTS songs;'
time_table_drop = 'DROP TABLE IF EXISTS time;'

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE event_staging (
    artist          TEXT,
    auth            TEXT        NOT NULL,
    firstName       TEXT,
    gender          CHAR(1),
    itemInSession   INTEGER     NOT NULL,
    lastName        TEXT,
    length          NUMERIC,
    level           TEXT        NOT NULL,
    location        TEXT,
    method          TEXT        NOT NULL,
    page            TEXT        NOT NULL,
    registration    NUMERIC,
    sessionId       INTEGER     NOT NULL,
    song            VARCHAR,
    status          INTEGER     NOT NULL,
    ts              NUMERIC     NOT NULL,
    userAgent       VARCHAR,
    userId          INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE song_staging (
    artist_id        TEXT      NOT NULL,
    artist_latitude  TEXT,
    artist_longitude TEXT,
    artist_location  TEXT,
    artist_name      TEXT      NOT NULL,
    duration         NUMERIC   NOT NULL,
    num_songs        INTEGER   NOT NULL,
    song_id          TEXT      NOT NULL,
    title            TEXT      NOT NULL,
    year             INTEGER   NOT NULL
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
  songplay_id    INTEGER         IDENTITY(0, 1)     PRIMARY KEY,
  start_time     TIMESTAMP       NOT NULL,
  user_id        INTEGER         NOT NULL,
  level          TEXT            NOT NULL,
  song_id        TEXT,
  artist_id      TEXT,
  session_id     INTEGER         NOT NULL,
  location       TEXT,
  user_agent     TEXT            NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE users (
  user_id        INTEGER                            PRIMARY KEY,
  first_name     TEXT            NOT NULL,
  last_name      TEXT            NOT NULL,
  gender         CHAR(1)         NOT NULL,
  level          TEXT            NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE songs (
  song_id        TEXT                               PRIMARY KEY,
  title          TEXT            NOT NULL,
  artist_id      TEXT            NOT NULL,
  year           INTEGER         NOT NULL,
  duration       NUMERIC         NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE artists (
  artist_id     TEXT                                PRIMARY KEY,
  name          TEXT             NOT NULL,
  location      TEXT,
  latitude      NUMERIC,
  longitude     NUMERIC
);
""")

time_table_create = ("""
CREATE TABLE time (
  start_time    TIMESTAMP                           PRIMARY KEY,
  hour          INTEGER          NOT NULL,
  day           INTEGER          NOT NULL,
  week          INTEGER          NOT NULL,
  month         INTEGER          NOT NULL,
  year          INTEGER          NOT NULL,
  weekday       INTEGER          NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = get_copy_json_query('event_staging', S3_LOG_DATA, ARN, S3_LOG_JSONPATH)

staging_songs_copy = get_copy_json_query('song_staging', S3_SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, user_id, level, song_id, artist_id,
    session_id, location, user_agent
)
SELECT
    timestamp 'epoch' + es.ts / 1000 * interval '1 second' AS start_time,
    es.userId                                              AS user_id,
    es.level,
    ss.song_id,
    ss.artist_id,
    es.sessionId as session_id,
    es.location,
    es.userAgent as user_agent
FROM
    event_staging AS es
JOIN
    song_staging AS ss ON (
        es.song = ss.title AND es.artist = ss.artist_name
    )
WHERE
    es.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (
    user_id, first_name, last_name,
    gender, level
)
SELECT
    es.userId,
    es.firstName,
    es.lastName,
    es.gender,
    es.level
FROM
    event_staging AS es
JOIN (
    SELECT
        max(ts) AS ts,
        userId
    FROM
        event_staging
    WHERE
        page = 'NextSong'
    GROUP BY
        userId
    ) AS max_ts_for_user
ON (
    es.userId = max_ts_for_user.userId AND es.ts = max_ts_for_user.ts
);
""")

song_table_insert = ("""
INSERT INTO songs
SELECT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM
    song_staging;
""")

artist_table_insert = ("""
INSERT INTO artists (               
    artist_id, name, location,
    latitude, longitude
)
SELECT DISTINCT
    ss.artist_id                AS artist_id,
    ss.artist_name              AS name,
    ss.artist_location          AS location,
    ss.artist_latitude          AS latitude,
    ss.artist_longitude         AS longitude
FROM
    song_staging AS ss;
""")

time_table_insert = ("""
INSERT INTO time (
    start_time, hour, day, week,
    month, year, weekday
)
SELECT
    timestamp_eval.start_time                       AS start_time,
    EXTRACT(hour FROM timestamp_eval.start_time)    AS hour,
    EXTRACT(day FROM timestamp_eval.start_time)     AS day,
    EXTRACT(week FROM timestamp_eval.start_time)    AS week,
    EXTRACT(month FROM timestamp_eval.start_time)   AS month,
    EXTRACT(year FROM timestamp_eval.start_time)    AS year,
    EXTRACT(weekday FROM timestamp_eval.start_time) AS weekday
FROM (
    SELECT DISTINCT
        TIMESTAMP 'epoch' + event_staging.ts/1000 * INTERVAL '1 second' AS start_time
    FROM 
        event_staging
    WHERE
        page = 'NextSong'
) timestamp_eval;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
