import asyncio
from datetime import timedelta
from io import BytesIO

from PIL import Image
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

    return {
        "gcs_uri": gcs_uri,
        "signed_url": url,
    }


def parse_gcs_uri(gcs_uri: str) -> tuple[str, str]:
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gcs_uri}")
    path = gcs_uri[5:]
    bucket_name, blob_name = path.split("/", 1)
    return bucket_name, blob_name


def download_bytes_from_gcs(gcs_uri: str) -> bytes:
    bucket_name, blob_name = parse_gcs_uri(gcs_uri)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.download_as_bytes()


def upload_pil_image_to_gcs(image: Image.Image, gcs_uri: str, format: str = "PNG") -> str:
    bucket_name, blob_name = parse_gcs_uri(gcs_uri)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    buf = BytesIO()
    if format.upper() == "JPEG":
        image.convert("RGB").save(buf, format="JPEG", quality=90)
        content_type = "image/jpeg"
    else:
        image.save(buf, format="PNG")
        content_type = "image/png"

    buf.seek(0)
    blob.upload_from_string(buf.getvalue(), content_type=content_type)
    return gcs_uri


def upload_bytes_to_gcs(data: bytes, gcs_uri: str, content_type: str = "application/octet-stream") -> str:
    bucket_name, blob_name = parse_gcs_uri(gcs_uri)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data, content_type=content_type)
    return gcs_uri


async def upload_pil_image_to_gcs_async(image: Image.Image, gcs_uri: str, format: str = "PNG") -> str:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, upload_pil_image_to_gcs, image, gcs_uri, format)


async def upload_bytes_to_gcs_async(data: bytes, gcs_uri: str, content_type: str = "application/octet-stream") -> str:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, upload_bytes_to_gcs, data, gcs_uri, content_type)


def build_detection_artifact_gcs_uris(
    frame_gcs_uri: str,
    detection_id: str,
) -> tuple[str, str]:
    bucket_name, blob_name = parse_gcs_uri(frame_gcs_uri)

    if "/frames/" not in blob_name:
        raise ValueError(f"frame_gcs_uri does not contain '/frames/': {frame_gcs_uri}")

    prefix = blob_name.split("/frames/")[0]

    crop_gcs_uri = f"gs://{bucket_name}/{prefix}/detections/{detection_id}/crop.jpg"
    mask_gcs_uri = f"gs://{bucket_name}/{prefix}/detections/{detection_id}/mask.png"

    return crop_gcs_uri, mask_gcs_uri
