

WITH source_data as (
SELECT * from {{ ref('stg_tracks_deduped') }}

),

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

buckets as(
SELECT
track_id,
        -- Popularity bucket
    case
        when popularity_decile <= 2 then 'Top 20%'
        when popularity_decile <= 5 then 'Middle'
        else 'Bottom 50%'
    end as popularity_bucket,

    -- Danceability bucket
    case
        when danceability_decile <= 2 then 'Very High'
        when danceability_decile <= 4 then 'High'
        when danceability_decile <= 6 then 'Medium'
        when danceability_decile <= 8 then 'Low'
        else 'Very Low'
    end as danceability_bucket,

    -- Energy bucket
    case
        when energy_decile <= 2 then 'Very High'
        when energy_decile <= 4 then 'High'
        when energy_decile <= 6 then 'Medium'
        when energy_decile <= 8 then 'Low'
        else 'Very Low'
    end as energy_bucket,

    -- Speechiness bucket
    case
        when speechiness_decile <= 3 then 'Highly Spoken'
        when speechiness_decile <= 7 then 'Mixed'
        else 'Mostly Musical'
    end as speechiness_bucket,

    -- Acousticness bucket
    case
        when acousticness_decile <= 3 then 'Highly Acoustic'
        when acousticness_decile <= 7 then 'Moderate'
        else 'Low Acoustic'
    end as acousticness_bucket,

    -- Instrumentalness bucket
    case
        when instrumentalness_decile <= 3 then 'Highly Instrumental'
        when instrumentalness_decile <= 7 then 'Mixed'
        else 'Vocal'
    end as instrumentalness_bucket,

    -- Liveness bucket
    case
        when liveness_decile <= 3 then 'Live'
        when liveness_decile <= 7 then 'Mixed'
        else 'Studio'
    end as liveness_bucket,

    -- Valence bucket
    case
        when valence_decile <= 2 then 'Very Positive'
        when valence_decile <= 4 then 'Positive'
        when valence_decile <= 6 then 'Neutral'
        when valence_decile <= 8 then 'Negative'
        else 'Very Negative'
    end as valence_bucket
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

    mode_numerical,
    mode_name,
    key_numerical,
    key_name,

    tempo,
    time_signature,

    # track ratings #
    popularity,
    popularity_bucket, 
    danceability,
    danceability_bucket,
    energy,
    energy_bucket,
    speechiness,
    speechiness_bucket,
    acousticness,
    acousticness_bucket,  
    instrumentalness,
    instrumentalness_bucket,
    liveness,
    liveness_bucket,
    valence,
    valence_bucket


FROM source_data as s

LEFT JOIN {{ ref('stg_track_genre_mapping') }} AS mapping
on s.track_id = mapping.track_id

LEFT JOIN rating_buckets AS ratings
on s.track_id = ratings.track_id

