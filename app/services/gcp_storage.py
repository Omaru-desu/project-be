from datetime import timedelta

from google.cloud import storage

client = storage.Client()
ACTIVE_BUCKET = "active-omaru"   
TEST_BUCKET = "test-omaru"  

def get_bucket_name(project_type: str) -> str:
    if project_type == "test":
        return TEST_BUCKET
    return ACTIVE_BUCKET

def upload_to_gcp(file_bytes, bucket_name, destination_blob_name, content_type):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(file_bytes, content_type=content_type)
    
    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=15),
        method="GET",
    )
    gcs_uri = f"gs://{bucket_name}/{destination_blob_name}"

    return  {
        "gcs_uri": gcs_uri,
        "signed_url": url,
    }