import random
import uuid
import requests
from concurrent.futures import ThreadPoolExecutor
import os
import re

# url = 'http://localhost:5000/download_ndjson'
headers = {
    'Accept': 'application/x-ndjson'  
}

chunk_size = 1024 * 1024  

def get_filename_from_headers(headers):
    content_disposition = headers.get('Content-Disposition')
    if content_disposition:
        filename_match = re.findall(r'filename="?([^";]+)"?', content_disposition)
        if filename_match:
            return filename_match[0]

    return 'downloaded_file.ndjson'

def download_chunk(url, start, end, chunk_index):
    range_header = {'Range': f'bytes={start}-{end}'}
    response = requests.get(url, headers={**headers, **range_header})
    
    if response.status_code in [200, 206]:
        random_postfix = str(uuid.uuid4()).replace('-', '_')
        chunk_filename = f'chunk_{chunk_index}_{random_postfix}.ndjson'
        with open(chunk_filename, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded chunk {chunk_index}")
        return chunk_filename
    else:
        print(f"Failed to download chunk {chunk_index}, Status code: {response.status_code}")
        return None

def multithread_download(url, **context):
    response = requests.head(url, headers=headers)

    if 'Content-Length' in response.headers:
        file_size = int(response.headers['Content-Length'])
    else:
        file_size = None

    filename = get_filename_from_headers(response.headers)

    if file_size:
        total_chunks = (file_size // chunk_size) + (1 if file_size % chunk_size > 0 else 0)
    else:
        total_chunks = None

    if total_chunks:
        chunk_files = []
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for i in range(total_chunks):
                start = i * chunk_size
                end = start + chunk_size - 1 if i < total_chunks - 1 else '' 
                futures.append(executor.submit(download_chunk, url, start, end, i))
            
            for future in futures:
                chunk_filename = future.result()
                if chunk_filename:
                    chunk_files.append(chunk_filename)

    with open(f'/opt/airflow/dags/{filename}', 'wb') as outfile:
        for chunk_filename in chunk_files:
            with open(chunk_filename, 'rb') as infile:
                outfile.write(infile.read())

    for chunk_filename in chunk_files:
        os.remove(chunk_filename)
        # print(f"Deleted {chunk_filename}")

    print(f"Download successfully and save at file {filename}")
    context['ti'].xcom_push(key='file_path', value=f'/opt/airflow/dags/{filename}')
    # context['ti'].xcom_push(key='file_name', value=f'{filename}')
