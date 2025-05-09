{% macro code_name_joined(code_grp_id, code_col) %}
(
    select code_nm
    from {{ source('code_master_igisdms', 'mdbmcodef') }} code
    where code.code_grp_id = '{{ code_grp_id }}'
      and code.code_id = {{ code_col }}
    limit 1
)
{% endmacro %}