## this table categorises some of our data, sorting it into buckets ##


{{
  config(materialized='view', tags=['intermediate'])
}}

with source_data as (
  select * from {{ ref('stg_tracks_deduped') }}
),

scored as (
  select * from {{ ref('int_tracks_scored') }}
)

select
  s.track_id,

  -- popularity
  case
    when sc.popularity_decile <= 2 then 'Top 20%'
    when sc.popularity_decile <= 7 then 'Middle 50%'
    else 'Bottom 30%'
  end as popularity_bucket,

  case when sc.popularity_decile = 1 then 1 else 0 end as is_top_10_percent,

  -- audio feature buckets (1 = best because deciles are DESC)
  case
    when sc.danceability_decile <= 2 then 'Very High'
    when sc.danceability_decile <= 4 then 'High'
    when sc.danceability_decile <= 6 then 'Medium'
    when sc.danceability_decile <= 8 then 'Low'
    else 'Very Low'
  end as danceability_bucket,

  case
    when sc.energy_decile <= 2 then 'Very High'
    when sc.energy_decile <= 4 then 'High'
    when sc.energy_decile <= 6 then 'Medium'
    when sc.energy_decile <= 8 then 'Low'
    else 'Very Low'
  end as energy_bucket,

  case
    when sc.speechiness_decile <= 3 then 'High Speech'
    when sc.speechiness_decile <= 7 then 'Mixed'
    else 'Low Speech'
  end as speechiness_bucket,

  case
    when sc.acousticness_decile <= 3 then 'High Acoustic'
    when sc.acousticness_decile <= 7 then 'Moderate'
    else 'Low Acoustic'
  end as acousticness_bucket,

  case
    when sc.instrumentalness_decile <= 3 then 'High Instrumental'
    when sc.instrumentalness_decile <= 7 then 'Mixed'
    else 'Vocal'
  end as instrumentalness_bucket,

  case
    when sc.liveness_decile <= 3 then 'High Liveness'
    when sc.liveness_decile <= 7 then 'Mixed'
    else 'Low Liveness'
  end as liveness_bucket,

  case
    when sc.valence_decile <= 2 then 'Very Positive'
    when sc.valence_decile <= 4 then 'Positive'
    when sc.valence_decile <= 6 then 'Neutral'
    when sc.valence_decile <= 8 then 'Negative'
    else 'Very Negative'
  end as valence_bucket,

  -- duration + tempo buckets (from raw numeric fields)
  case
    when s.duration_seconds < 120 then 'Short'
    when s.duration_seconds < 240 then 'Standard'
    when s.duration_seconds < 360 then 'Long'
    else 'Very Long'
  end as duration_bucket,

  case
    when s.tempo < 90 then 'Slow'
    when s.tempo < 120 then 'Medium'
    when s.tempo < 150 then 'Fast'
    else 'Very Fast'
  end as tempo_bucket

from source_data s
join scored sc
on s.track_id = sc.track_id