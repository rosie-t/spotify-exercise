## Macro to de-dupe tracks using track_id as partition, order by populatiry 

{% macro dedupe_relation(relation, partition_by, order_by) %}
select *
from {{ relation }}
qualify row_number() over ( partition by {{ partition_by | join(', ') }} order by {{ order_by | join(', ') }}) = 1
{% endmacro %}

