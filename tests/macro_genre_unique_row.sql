-- tests that each macro genre has its own row 

select
  macro_genre,
  count(*) as row_count
from {{ ref('fct_macro_genre') }}
group by 1
having count(*) > 1