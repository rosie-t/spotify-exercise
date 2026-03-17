{{
  config(materialized='table', tags=['mart', 'dim'])
}}


with source_data as (
  select * from {{ ref('stg_tracks_deduped') }}
),

scored as (
  select * from {{ ref('int_tracks_scored') }}
),

buckets as (
  select * from {{ ref('int_tracks_buckets') }}
),

genre as (
    select * from {{ ref('stg_track_genre_mapping')}}
),


top_10 as (
    select * from {{ ref('int_tracks_buckets')}}
    WHERE is_top_10_percent = 1
    
)

# one row per macro genre # 



SELECT

genre.macro_genre,

-- populous count

COUNT (genre.track_id) as count, -- counts how many songs have that macro genre
ROUND(SAFE_DIVIDE(COUNT(top_10.track_id), (COUNT(genre.track_id))), 2) as top_10_share, -- how much of the top 10 tracks are dominated by that genre


 -- how do the average z scores track, aka how unusual is each genre -- 
ROUND(AVG(scored.popularity_z), 2) AS avg_popularity_z,
ROUND(AVG(scored.danceability_z), 2) AS avg_danceability_z,
ROUND(AVG(scored.energy_z), 2) AS avg_energy_z,
ROUND(AVG(scored.speechiness_z), 2) AS avg_speechiness_z,
ROUND(AVG(scored.acousticness_z), 2) AS avg_acousticness_z,
ROUND(AVG(scored.instrumentalness_z), 2) AS avg_instrumentalness_z,
ROUND(AVG(scored.liveness_z), 2) AS avg_liveness_z,
ROUND(AVG(scored.valence_z), 2) AS avg_valence_z,

-- explicit rate

ROUND(COUNTIF(is_explicit) / COUNT(*), 2) as explicit_rate

FROM genre 

LEFT JOIN scored
ON genre.track_id = scored.track_id

LEFT JOIN top_10
ON genre.track_id = top_10.track_id

LEFT JOIN source_data
ON genre.track_id = source_data.track_id

GROUP BY 1