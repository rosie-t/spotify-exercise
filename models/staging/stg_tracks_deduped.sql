{{
  config(
    materialized='table',
    tags=['staging', 'clean_data']
  )
}}

with source as (

    select * from {{ source('spotify_exercise', 'raw_data') }}

),

ranked AS(
    SELECT
    *,
    row_number() over (partition by track_id order by popularity desc, track_genre, track_name) as row_number

    FROM SOURCE
    WHERE album_name IS NOT NULL

)



SELECT 


# primary key #
    track_id,

    # track qualitative info #
    track_name,
    artists AS artists_name,
    album_name,
    explicit AS is_explicit,
    track_genre AS genre,

    # track quantitative data #
    duration_ms,
    SAFE_DIVIDE(CAST(duration_ms AS INT64), 1000) AS duration_seconds,
    ROUND(SAFE_DIVIDE(CAST(duration_ms AS INT64), 60000), 3) AS duration_minutes,
    loudness,

    CAST(mode AS INT64) AS mode_numerical,
    # major or minor in words #
    CASE 
    WHEN mode = 1 THEN 'major'
    ELSE 'minor' END AS mode_name,

    CAST(key AS INT64) AS key_numerical,
    # key as words #
    CASE
    WHEN key = -1 THEN 'No key detected'
    WHEN key = 0 THEN 'C'
    WHEN key = 1 THEN 'C#/Db'
    WHEN key = 2 THEN 'D'
    WHEN key = 3 THEN 'D#/Eb'
    WHEN key = 4 THEN 'E'
    WHEN key = 5 THEN 'F'
    WHEN key = 6 THEN 'F#/Gb'
    WHEN key = 7 THEN 'G'
    WHEN key = 8 THEN 'G#/Ab'
    WHEN key = 9 THEN 'A'
    WHEN key = 10 THEN 'A#/Bb'
    WHEN key = 11 THEN 'B'
    ELSE NULL
    END AS key_name,

    tempo,
    time_signature,

    # track ratings #
    popularity, 
    danceability,
    energy,
    speechiness,
    acousticness,  
    instrumentalness,
    liveness,
    valence


FROM ranked

WHERE row_number = 1