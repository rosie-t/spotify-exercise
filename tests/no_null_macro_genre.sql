-- checks that all macro_genres are accounted for

select *
from {{ ref('fct_macro_genre') }}
where macro_genre is null