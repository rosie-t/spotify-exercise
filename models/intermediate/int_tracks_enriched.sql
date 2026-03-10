{{
  config(
    materialized='view',
    tags=['intermediate']
  )
}}

WITH source_data as (
SELECT * from {{ ref('stg_tracks_deduped') }}

),

## Gives the percentile for each track (/10) ##

percentiles as(

SELECT 
track_id,
    ntile(10) over (order by popularity desc) as popularity_decile,
    ntile(10) over (order by danceability desc) as danceability_decile,
    ntile(10) over (order by energy desc) as energy_decile,
    ntile(10) over (order by speechiness desc) as speechiness_decile,
    ntile(10) over (order by acousticness desc) as acousticness_decile,
    ntile(10) over (order by instrumentalness desc) as instrumentalness_decile,
    ntile(10) over (order by liveness desc) as liveness_decile,
    ntile(10) over (order by valence desc) as valence_decile

FROM {{ ref('stg_tracks_deduped') }}

),
## average and std dev for each rating category ##

feature_stats AS(
SELECT
    AVG(popularity) as avg_popularity, 
    STDDEV_POP(popularity) AS std_popularity,
    AVG(danceability) as avg_danceability, 
    STDDEV_POP(danceability) AS std_danceability,
    AVG(energy) as avg_energy, 
    STDDEV_POP(energy) AS std_energy,
    AVG(speechiness) as avg_speechiness, 
    STDDEV_POP(speechiness) AS std_speechiness,
    AVG(acousticness) as avg_acousticness, 
    STDDEV_POP(acousticness) AS std_acousticness,
    AVG(instrumentalness) as avg_instrumentalness, 
    STDDEV_POP(instrumentalness) AS std_instrumentalness,
    AVG(liveness) as avg_liveness, 
    STDDEV_POP(liveness) AS std_liveness,
    AVG(valence) as avg_valence, 
    STDDEV_POP(valence) AS std_valence

FROM {{ ref('stg_tracks_deduped') }}

),

## gives buckets for each attribute based on the decile ##

buckets as(
SELECT
track_id,
        -- Popularity bucket
    CASE
      WHEN popularity_decile <= 2 THEN 'Top 20%'
      WHEN popularity_decile <= 7 THEN 'Middle 50%'
      ELSE 'Bottom 30%'
    END AS popularity_bucket,

    CASE
      WHEN popularity_decile = 1 THEN 1
      ELSE 0
    END AS is_top_10_percent,

    -- Danceability bucket (1 = best)
    CASE
      WHEN danceability_decile <= 2 THEN 'Very High'
      WHEN danceability_decile <= 4 THEN 'High'
      WHEN danceability_decile <= 6 THEN 'Medium'
      WHEN danceability_decile <= 8 THEN 'Low'
      ELSE 'Very Low'
    END AS danceability_bucket,

    -- Energy bucket (1 = best)
    CASE
      WHEN energy_decile <= 2 THEN 'Very High'
      WHEN energy_decile <= 4 THEN 'High'
      WHEN energy_decile <= 6 THEN 'Medium'
      WHEN energy_decile <= 8 THEN 'Low'
      ELSE 'Very Low'
    END AS energy_bucket,

    -- Speechiness bucket (1 = most speechy, because DESC + 1=best)
    CASE
      WHEN speechiness_decile <= 3 THEN 'High Speech'
      WHEN speechiness_decile <= 7 THEN 'Mixed'
      ELSE 'Low Speech'
    END AS speechiness_bucket,

    -- Acousticness bucket
    CASE
      WHEN acousticness_decile <= 3 THEN 'High Acoustic'
      WHEN acousticness_decile <= 7 THEN 'Moderate'
      ELSE 'Low Acoustic'
    END AS acousticness_bucket,

    -- Instrumentalness bucket
    CASE
      WHEN instrumentalness_decile <= 3 THEN 'High Instrumental'
      WHEN instrumentalness_decile <= 7 THEN 'Mixed'
      ELSE 'Vocal'
    END AS instrumentalness_bucket,

    -- Liveness bucket
    CASE
      WHEN liveness_decile <= 3 THEN 'High Liveness'
      WHEN liveness_decile <= 7 THEN 'Mixed'
      ELSE 'Low Liveness'
    END AS liveness_bucket,

    -- Valence bucket
    CASE
      WHEN valence_decile <= 2 THEN 'Very Positive'
      WHEN valence_decile <= 4 THEN 'Positive'
      WHEN valence_decile <= 6 THEN 'Neutral'
      WHEN valence_decile <= 8 THEN 'Negative'
      ELSE 'Very Negative'
    END AS valence_bucket

    FROM percentiles
)


SELECT 

    # primary key
    s.track_id,

    s.track_name,
    s.artists_name,
    s.album_name,

    s.is_explicit,
    s.genre,
    mapping.macro_genre,

    s.duration_ms,
    duration_seconds,
    duration_minutes,
        case
        when duration_seconds < 120 then 'Short'
        when duration_seconds < 240 then 'Standard'
        when duration_seconds < 360 then 'Long'
        else 'Very Long'
    end as duration_bucket,

    mode_numerical,
    mode_name,
    key_numerical,
    key_name,

    tempo,
        case
        when tempo < 90 then 'Slow'
        when tempo < 120 then 'Medium'
        when tempo < 150 then 'Fast'
        else 'Very Fast'
    end as tempo_bucket,
    time_signature,

    # track ratings #
      s.popularity,
    b.popularity_bucket,
    b.is_top_10_percent,
    p.popularity_decile,

    s.danceability,
    b.danceability_bucket,
    p.danceability_decile,

    s.energy,
    b.energy_bucket,
    p.energy_decile,

    s.speechiness,
    b.speechiness_bucket,
    p.speechiness_decile,

    s.acousticness,
    b.acousticness_bucket,
    p.acousticness_decile,

    s.instrumentalness,
    b.instrumentalness_bucket,
    p.instrumentalness_decile,

    s.liveness,
    b.liveness_bucket,
    p.liveness_decile,

    s.valence,
    b.valence_bucket,
    p.valence_decile,




    ## z scores - how far away from the mean is something ##

    SAFE_DIVIDE(s.popularity - fs.avg_popularity, NULLIF(fs.std_popularity, 0)) AS popularity_z,
    SAFE_DIVIDE(s.danceability - fs.avg_danceability, NULLIF(fs.std_danceability, 0)) AS danceability_z,
    SAFE_DIVIDE(s.energy - fs.avg_energy, NULLIF(fs.std_energy, 0)) AS energy_z,
    SAFE_DIVIDE(s.speechiness - fs.avg_speechiness, NULLIF(fs.std_speechiness, 0)) AS speechiness_z,
    SAFE_DIVIDE(s.acousticness - fs.avg_acousticness, NULLIF(fs.std_acousticness, 0)) AS acousticness_z,
    SAFE_DIVIDE(s.instrumentalness - fs.avg_instrumentalness, NULLIF(fs.std_instrumentalness, 0)) AS instrumentalness_z,
    SAFE_DIVIDE(s.liveness - fs.avg_liveness, NULLIF(fs.std_liveness, 0)) AS liveness_z,
    SAFE_DIVIDE(s.valence - fs.avg_valence, NULLIF(fs.std_valence, 0)) AS valence_z


FROM source_data as s

LEFT JOIN {{ ref('stg_track_genre_mapping') }} AS mapping
on s.track_id = mapping.track_id

LEFT JOIN buckets AS b
on s.track_id = b.track_id

LEFT JOIN percentiles AS p
ON s.track_id = p.track_id

CROSS JOIN feature_stats as fs

