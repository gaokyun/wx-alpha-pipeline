{% macro create_data_pump_dir() %}
{% set sql %}
BEGIN
    EXECUTE IMMEDIATE 'CREATE OR REPLACE DIRECTORY DATA_PUMP_DIR AS ''/tmp''';
END;
{% endset %}
{{ run_query(sql) }}
{% endmacro %}
