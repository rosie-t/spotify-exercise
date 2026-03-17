{{
  config(materialized='table', tags=['mart'])
}}

with source_data as (
  select * from {{ ref('stg_tracks_deduped') }}
),

scored as (
  select * from {{ ref('int_tracks_scored') }}
),

buckets as (
  select * from {{ ref('int_tracks_buckets') }}
)

select
  -- PK
  s.track_id,

  -- descriptive
  s.track_name,
  s.artists_name,
  s.album_name,

  s.is_explicit,
  s.genre,
  m.macro_genre,

  -- time-ish
  s.duration_ms,
  s.duration_seconds,
  s.duration_minutes,
  b.duration_bucket,

  -- musical properties
  s.mode_numerical,
  s.mode_name,
  s.key_numerical,
  s.key_name,

  s.tempo,
  b.tempo_bucket,
  s.time_signature,

  -- raw audio features
  s.popularity,
  s.danceability,
  s.energy,
  s.speechiness,
  s.acousticness,
  s.instrumentalness,
  s.liveness,
  s.valence,

  -- deciles
  sc.popularity_decile,
  sc.danceability_decile,
  sc.energy_decile,
  sc.speechiness_decile,
  sc.acousticness_decile,
  sc.instrumentalness_decile,
  sc.liveness_decile,
  sc.valence_decile,

  -- z-scores
  sc.popularity_z,
  sc.danceability_z,
  sc.energy_z,
  sc.speechiness_z,
  sc.acousticness_z,
  sc.instrumentalness_z,
  sc.liveness_z,
  sc.valence_z,

  -- buckets + flags
  b.popularity_bucket,
  b.is_top_10_percent,
  b.danceability_bucket,
  b.energy_bucket,
  b.speechiness_bucket,
  b.acousticness_bucket,
  b.instrumentalness_bucket,
  b.liveness_bucket,
  b.valence_bucket

from source_data s
left join {{ ref('stg_track_genre_mapping') }} m
  on s.track_id = m.track_id
left join scored sc
  on s.track_id = sc.track_id
left join buckets b
  on s.track_id = b.track_id