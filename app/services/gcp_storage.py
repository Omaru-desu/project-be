from datetime import timedelta

from google.cloud import storage

client = storage.Client()
bucket_name = "test-omaru"

def upload_to_gcp(file_bytes, destination_blob_name, content_type):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(file_bytes, content_type=content_type)
    
    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=15),
        method="GET",
    )

    return url