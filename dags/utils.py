import ndjson
import csv
import gzip
import os
from google.cloud import storage

def ndjson_to_csv(**context):
  file_path = context['ti'].xcom_pull(task_ids='download_task', key='file_path')
  csv_file = file_path[:-6] + 'csv'
  ndjson_file = file_path
  with open(ndjson_file, 'r') as ndjson_f, open(csv_file, 'w', newline='') as csv_f:
    fieldnames = set()
    with open(ndjson_file, 'r') as ndjson_f:
      reader = ndjson.reader(ndjson_f)
      for obj in reader:
          fieldnames.update(obj.keys())

    fieldnames = sorted(fieldnames)

    with open(ndjson_file, 'r') as ndjson_f, open(csv_file, 'w', newline='') as csv_f:
      reader = ndjson.reader(ndjson_f)
      writer = csv.DictWriter(csv_f, fieldnames=fieldnames)
      writer.writeheader()
      rows_written = 0
      for obj in reader:
        try:
            writer.writerow(obj)
            rows_written += 1
        except Exception as e:
            print(f"Error writing row {obj}: {e}")
  
  context['ti'].xcom_push(key='file_path', value=csv_file)
        
def csv_to_gzip(**context):
  file_path = context['ti'].xcom_pull(task_ids='ndjson_to_csv_task', key='file_path')
  csv_file = file_path
  gzip_file = file_path[:-3] + 'gzip'
  with open(csv_file, 'rb') as f_in, gzip.open(gzip_file, 'wb') as f_out:
    while chunk := f_in.read(1024 * 1024):
      f_out.write(data=chunk)
  
  context['ti'].xcom_push(key='file_path', value=gzip_file)
  context['ti'].xcom_push(key='file_name', value=os.path.basename(gzip_file))


def check_bucket_existence(bucket_name):
  storage_client = storage.Client()
  bucket = storage_client.lookup_bucket(bucket_name)
  if bucket:
      return True
  return False