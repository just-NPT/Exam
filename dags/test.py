from google.cloud import storage

def connect_to_gcs(service_account_key_path):
    # Create a storage client using the service account key
    storage_client = storage.Client.from_service_account_json(service_account_key_path)
    
    # Return the client object
    return storage_client

# Path to your service account key JSON file
service_account_key_path = './key_file.json'

# Connect to GCS
gcs_client = connect_to_gcs(service_account_key_path)

# Now you can use gcs_client to interact with GCS
# Example: List all the buckets in the project
buckets = list(gcs_client.list_buckets())
for bucket in buckets:
    print(bucket.name)