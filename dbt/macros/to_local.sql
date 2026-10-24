{% macro to_local(ts) -%}
    ({{ ts }} + interval {{ var('local_utc_offset_hours') }} hour)
{%- endmacro %}
