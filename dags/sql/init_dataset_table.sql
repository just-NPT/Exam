CREATE SCHEMA IF NOT EXISTS `{{params.project_id}}.{{params.dataset_id}}`;

CREATE TABLE IF NOT EXISTS `{{params.project_id}}.{{params.dataset_id}}.{{params.table_name}}`
(
  access STRING,
  annotations STRING,
  cases STRING,
  data_category STRING,
  data_format STRING,
  file_name STRING,
  file_size INT64
);  