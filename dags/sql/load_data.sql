LOAD DATA INTO TABLE `{{ params.project_id }}.{{ params.dataset_id }}.{{ params.table_name }}`(
  access,
  annotations,
  cases,
  data_category,
  data_format,
  file_name,
  file_size
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://{{ params.bucket_name }}/{{ params.file_path }}'],
  skip_leading_rows = 1,
  field_delimiter = ',',
  compression = 'GZIP'
);