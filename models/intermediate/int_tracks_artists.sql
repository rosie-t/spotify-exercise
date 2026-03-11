{{
  config(
    materialized='view',
    tags=['intermediate']
  )
}}

with source_data as (
  select
    track_id,
    artists_name
  from {{ ref('stg_tracks_deduped') }}
),

split_artists as (
  select
    track_id,
    split(artists_name, ';') as artists_array
  from source_data
),

unnested as (
  select
    track_id,
    trim(artist_name) as artist_name,
    cast(artist_position as int64) as artist_position
  from split_artists,
  unnest(artists_array) as artist_name with offset as artist_position
)

select
  track_id,
  artist_name,
  artist_position
from unnested
where artist_name is not null
  and artist_name != ''
qualify row_number() over (
  partition by track_id, artist_name
  order by artist_position
) = 1
