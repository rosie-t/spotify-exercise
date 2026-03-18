{{
  config(
    materialized='table',
    tags=['mart']
  )
}}

with artists as (
  select
    *
  from {{ ref('int_tracks_artists') }}
),

tracks as (
  select
    *
  from {{ ref('fct_tracks_enriched') }}
)

SELECT

a.artist_name,
a.track_id,
t.album_name,
t.track_name,
t.macro_genre as track_genre



FROM artists a
LEFT JOIN tracks t
ON a.track_id = t.track_id

