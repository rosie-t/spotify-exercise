## this table does all our calculations and is as a separate int so we can run unit tests ##

{{
  config(materialized='view', 
  tags=['intermediate'])
}}

with source_data as (
  select * from {{ ref('stg_tracks_deduped') }}
),

percentiles as (
  select
    track_id,
    ntile(10) over (order by popularity desc)        as popularity_decile,
    ntile(10) over (order by danceability desc)     as danceability_decile,
    ntile(10) over (order by energy desc)           as energy_decile,
    ntile(10) over (order by speechiness desc)      as speechiness_decile,
    ntile(10) over (order by acousticness desc)     as acousticness_decile,
    ntile(10) over (order by instrumentalness desc) as instrumentalness_decile,
    ntile(10) over (order by liveness desc)         as liveness_decile,
    ntile(10) over (order by valence desc)          as valence_decile
  from source_data
),

feature_stats as (
  select
    avg(popularity) as avg_popularity,
    stddev_pop(popularity) as std_popularity,
    avg(danceability) as avg_danceability,
    stddev_pop(danceability) as std_danceability,
    avg(energy) as avg_energy,
    stddev_pop(energy) as std_energy,
    avg(speechiness) as avg_speechiness,
    stddev_pop(speechiness) as std_speechiness,
    avg(acousticness) as avg_acousticness,
    stddev_pop(acousticness) as std_acousticness,
    avg(instrumentalness) as avg_instrumentalness,
    stddev_pop(instrumentalness) as std_instrumentalness,
    avg(liveness) as avg_liveness,
    stddev_pop(liveness) as std_liveness,
    avg(valence) as avg_valence,
    stddev_pop(valence) as std_valence
  from source_data
)

select
  s.track_id,

  -- deciles
  p.popularity_decile,
  p.danceability_decile,
  p.energy_decile,
  p.speechiness_decile,
  p.acousticness_decile,
  p.instrumentalness_decile,
  p.liveness_decile,
  p.valence_decile,

  -- z-scores
  round(safe_divide(s.popularity - fs.avg_popularity, nullif(fs.std_popularity, 0)), 6) as popularity_z,
  round(safe_divide(s.danceability - fs.avg_danceability, nullif(fs.std_danceability, 0)), 6) as danceability_z,
  round(safe_divide(s.energy - fs.avg_energy, nullif(fs.std_energy, 0)), 6) as energy_z,
  round(safe_divide(s.speechiness - fs.avg_speechiness, nullif(fs.std_speechiness, 0)), 6) as speechiness_z,
  round(safe_divide(s.acousticness - fs.avg_acousticness, nullif(fs.std_acousticness, 0)), 6) as acousticness_z,
  round(safe_divide(s.instrumentalness - fs.avg_instrumentalness, nullif(fs.std_instrumentalness, 0)), 6) as instrumentalness_z,
  round(safe_divide(s.liveness - fs.avg_liveness, nullif(fs.std_liveness, 0)), 6) as liveness_z,
  round(safe_divide(s.valence - fs.avg_valence, nullif(fs.std_valence, 0)), 6) as valence_z

from source_data AS s
join percentiles AS p
  on s.track_id = p.track_id
cross join feature_stats AS fs