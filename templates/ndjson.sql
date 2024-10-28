{{fq_sql_macros}}

COPY (
	WITH transformed AS (
		SELECT {{fq_sql_transform_expression}} AS result 
		FROM read_json_auto(
			'{{fq_input_dir}}/**/*{{fq_vd_resource}}*.ndjson'
			{{fq_sql_input_schema}}
		)
		{{fq_where_filter}}
	)
	SELECT {{fq_sql_flattening_cols}}
	FROM transformed
	{{fq_sql_flattening_tables}}
)
TO '{{fq_output_dir}}/{{fq_vd_name}}.ndjson' 
(FORMAT JSON);