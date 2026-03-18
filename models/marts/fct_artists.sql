{{
  config(materialized='table', tags=['mart',])
}}


with artists as (
  select * from {{ ref('int_tracks_artists') }}
),

tracks as (
  select * from {{ ref('fct_tracks_enriched') }}
),

## metrics by artist, averaged ##
artist_metrics as (
  select
    a.artist_name,
    count(distinct a.track_id) as track_count,
    ROUND(avg(t.popularity), 2) as avg_popularity,
    ROUND(max(t.popularity), 2) as max_popularity,
    ROUND(avg(t.danceability), 2) as avg_danceability,
    ROUND(max(t.danceability), 2) as max_danceability,
    ROUND(avg(t.energy), 2) as avg_energy,
    ROUND(max(t.energy), 2) as max_energy,
    ROUND(avg(t.speechiness), 2) as avg_speechiness,
    ROUND(max(t.speechiness), 2) as max_speechiness,
    ROUND(avg(t.acousticness), 2) as avg_acousticness,
    ROUND(max(t.acousticness), 2) as max_acousticness,
    ROUND(avg(t.instrumentalness), 2) as avg_instrumentalness,
    ROUND(max(t.instrumentalness), 2) as max_instrumentalness,
    ROUND(avg(t.liveness), 2) as avg_liveness,
    ROUND(max(t.liveness), 2) as max_liveness,
    ROUND(avg(t.valence), 2) as avg_valence,
    ROUND(max(t.valence), 2) as max_valence,

    countif(t.is_top_10_percent = 1) as top_10_track_count,
    ROUND(safe_divide(countif(t.is_top_10_percent = 1), count(distinct a.track_id)), 2) as top_10_share,

## average of the z scores ##
    ROUND(avg(t.popularity_z), 2) as avg_popularity_z,
    ROUND(avg(t.danceability_z), 2) as avg_danceability_z,
    ROUND(avg(t.energy_z), 2) as avg_energy_z,
    ROUND(avg(t.speechiness_z), 2) as avg_speechiness_z,
    ROUND(avg(t.acousticness_z), 2) as avg_acousticness_z,
    ROUND(avg(t.instrumentalness_z), 2) as avg_instrumentalness_z,
    ROUND(avg(t.liveness_z), 2) as avg_liveness_z,
    ROUND(avg(t.valence_z), 2) as avg_valence_z

    FROM artists AS a
    LEFT JOIN tracks AS t 
    on a.track_id = t.track_id
    GROUP BY 1
),

final as(
  select
    *,
    ntile(10) over (order by avg_popularity desc) as popularity_decile,
    ntile(10) over (order by avg_danceability desc)     as danceability_decile,
    ntile(10) over (order by avg_energy desc)           as energy_decile,
    ntile(10) over (order by avg_speechiness desc)      as speechiness_decile,
    ntile(10) over (order by avg_acousticness desc)     as acousticness_decile,
    ntile(10) over (order by avg_instrumentalness desc) as instrumentalness_decile,
    ntile(10) over (order by avg_liveness desc)         as liveness_decile,
    ntile(10) over (order by avg_valence desc)          as valence_decile
  from artist_metrics)

select * from final