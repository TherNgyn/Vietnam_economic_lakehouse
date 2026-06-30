{% macro sk(fields) %}
    abs(xxhash64(
        {%- for field in fields -%}
            coalesce(cast({{ field }} as string), '__null__')
            {%- if not loop.last %}, {% endif -%}
        {%- endfor -%}
    ))
{% endmacro %}