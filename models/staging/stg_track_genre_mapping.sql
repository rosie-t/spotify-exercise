{{
  config(
    materialized='table',
    tags=['staging', 'clean_data', 'mapping']
  )
}}


with deduped as (

  {{ dedupe_relation(
      source('spotify_exercise', 'raw_data'),
      ['track_id'],
      ['popularity desc', 'track_genre', 'track_name']
  ) }}

),

cleaned as (

  select
    track_id,
    lower(trim(track_genre)) as track_genre
  from deduped
  where track_id is not null

)


select
  track_id,
  track_genre,
case
  /* ROCK family */
  when track_genre in ('rock','alt-rock','alt rock','psych-rock','psych rock','hard-rock','hard rock',
   'rock-n-roll','rock n roll','rockabilly','garage','grunge','emo','punk','punk-rock',
    'punk rock','punk-rock','garage','indie','indie-pop','indie pop','british','psych-rock',
    'power-pop','power pop','pop-rock','alt rock','alternative','emo','goth','rock-n-roll')
    then 'Rock'

  /* METAL family */
  when track_genre in ('metal','heavy-metal','heavy metal','black-metal','black metal','death-metal','death metal',
             'metalcore','grindcore','grind core','hardcore','hard-core')
    then 'Metal'

  /* ELECTRONIC / EDM (including house/techno/trance/bass subgenres) */
  when track_genre in ('electronic','edm','techno','minimal-techno','minimal techno','detroit-techno','detroit techno',
   'house','deep-house','deep house','progressive-house','progressive house','chicago-house',
   'chicago house','hardstyle','trance','dance','club','club','electro','electro','synth-pop',
   'synth pop','progressive-house','progressive house','idm','idm','breakbeat','break beat',
   'dubstep','dub step','drum-and-bass','drum and bass','drum & bass','drum and bass','dnb',
   'trance','techno','minimal techno','detroit techno','edm','progressive house','deep house',
    'house','progressive-house','hardstyle','breakbeat','break beat','dancehall' /* note: dancehall often sits between electronic & world */
    )
    then 'Electronic / Dance'

  /* DANCE / POP (mainstream dance + pop overlaps) */
  when track_genre in ('pop','pop-film','pop film','disco','dance','synth-pop','synth pop','power-pop','power pop',
   'indie-pop','indie pop','dance','party','club','pop-film','show-tunes','show tunes','dancehall')
    then 'Pop / Dance'

  /* HIP-HOP, R&B, SOUL, FUNK */
  when track_genre in ('hip-hop','hip hop','r-n-b','rnb','r & b','r & b','rnb','r-n-b','soul','funk','groove','gospel')
    then 'Hip-Hop / R&B / Soul'

  /* LATIN & BRAZIL / regional latin styles — put 'latino' in here per your note */
  when track_genre in ('latin','latino','latino','latino','latino','latin','latino','salsa','samba','reggaeton',
    'reggaeton','brazil','mpb','mpb','forro','forró','pagode','pagode','sertanejo','tango','samba',
    'salsa','bossa nova') /* bossa nova optional if present */
    then 'Latin & Brazilian'

  /* WORLD & REGIONAL (language-based / regional pop, asian pop categories, misc world) */
  when track_genre in ('j-pop','jpop','j-pop','j-pop','j-rock','jrock','j-idol','j-idol','j-dance','k-pop','kpop',
    'cantopop','cantopop','mandopop','mandopop','anime','anime','k-pop','kpop',
    'j-pop','j-pop','j-rock','j-rock','cantopop','mandopop','k-pop','kpop',
    'french','swedish','german','turkish','iranian','indian','malay','malay','spanish','spanish',
    'japanese','chinese','indian','iranian','german','french','swedish','turkish','malay'
    )
    then 'World / Regional'

  /* REGGAE & SKA family */
  when track_genre in ('reggae','dub','dubstep','dub step','ska','ska','dancehall')
    then 'Reggae / Ska / Bass'

  /* JAZZ, BLUES, COUNTRY, ROOTS */
  when track_genre in ('jazz','blues','bluegrass','country','honky-tonk','folk','folk','singer-songwriter','singer songwriter')
    then 'Jazz / Blues / Country / Folk'

  /* CLASSICAL & INSTRUMENTAL */
  when track_genre in ('classical','opera','piano','guitar','ambient','new-age','new age','acoustic','instrumental','study')
    then 'Classical & Instrumental'

  /* SOFT / MOOD / FUNCTIONAL (happy/sad/chill/party/sleep/romance/kids) */
  when track_genre in ('happy','sad','chill','party','sleep','study','romance','romantic','kids','children','kids','children','disney')
    then 'Mood / Functional'

  /* CHILDREN / FAMILY / SHOW */
  when track_genre in ('show-tunes','show tunes','disney','children','kids')
    then 'Children / Family'

  /* COMEDY / NOVELTY */
  when track_genre in ('comedy')
    then 'Comedy / Novelty'

  /* INDIETYPES & ALTERNATIVE — small overlap handled above, catch-alls here */
  when track_genre in ('indie','indie-pop','indie pop','alt-rock','alternative','alternative rock','psych-rock','psych rock','rockabilly','garage','rock-n-roll')
    then 'Indie / Alternative'

  /* METAL subgenres already handled above, but include some leftovers */
  when track_genre in ('grindcore','metalcore','death-metal','death metal','black-metal','black metal')
    then 'Metal'

  /* MISC / OTHER (fallback) */
  else 'Other'
end as macro_genre

from cleaned