import asyncio
import base64
from io import BytesIO

from PIL import Image

from app.api.helper.upload import update_upload_record, update_frame_record
from app.api.helper.segment import insert_detection_records
from app.api.helper.embed import upsert_detection_embeddings, upsert_frame_embeddings
from app.services.gcp_storage import (
    upload_bytes_to_gcs_async,
    upload_pil_image_to_gcs_async,
    build_detection_artifact_gcs_uris,
)
from app.services.model_service import process_frames as call_model_process_frames


async def _upload_detection_artifacts(
    crop_image: Image.Image,
    crop_gcs_uri: str,
    mask_image: Image.Image,
    mask_gcs_uri: str,
) -> None:
    await asyncio.gather(
        upload_pil_image_to_gcs_async(crop_image, crop_gcs_uri, "JPEG"),
        upload_pil_image_to_gcs_async(mask_image, mask_gcs_uri, "PNG"),
    )


async def process_upload(
    upload_id: str,
    project_id: str,
    user_id: str,
    frame_records: list[dict],
    label_ids: list[str] | None,
    frame_bytes_map: dict[str, bytes],
) -> None:
    try:
        update_upload_record(upload_id, {"status": "segmenting"})

        chunk_size = 20
        total_frames = len(frame_records)
        frames_processed = 0

        for i in range(0, total_frames, chunk_size):
            chunk_records = frame_records[i : i + chunk_size]
            chunk_bytes_map = {}
            chunk_metadata = []
            chunk_uri_map = {}

            for frame in chunk_records:
                frame_id = frame["id"]
                frame_gcs_uri = frame["frame_gcs_uri"]
                chunk_uri_map[frame_id] = frame_gcs_uri
                chunk_bytes_map[frame_id] = frame_bytes_map[frame_id]
                chunk_metadata.append({
                    "frame_id": frame_id,
                    "project_id": project_id,
                    "upload_id": upload_id,
                })

            model_results = await call_model_process_frames(chunk_bytes_map, chunk_metadata, label_ids)

            detection_rows: list[dict] = []
            frame_embedding_rows: list[dict] = []
            detection_embedding_rows: list[dict] = []

            for frame_result in model_results:
                frame_id = frame_result["frame_id"]
                frame_gcs_uri = chunk_uri_map.get(frame_id, "")
                frame_detections = frame_result.get("detections", [])

                upload_pairs = []
                for det in frame_detections:
                    detection_id = det["detection_id"]

                    crop_image = Image.open(BytesIO(base64.b64decode(det["crop_image"]))).convert("RGB")
                    mask_image = Image.open(BytesIO(base64.b64decode(det["mask_image"]))).convert("L")

                    crop_gcs_uri, mask_gcs_uri = build_detection_artifact_gcs_uris(
                        frame_gcs_uri=frame_gcs_uri,
                        detection_id=detection_id,
                    )

                    upload_pairs.append((crop_image, crop_gcs_uri, mask_image, mask_gcs_uri, det))

                tasks = [
                    upload_bytes_to_gcs_async(chunk_bytes_map[frame_id], frame_gcs_uri, "image/jpeg")
                ]
                for (crop_img, crop_uri, mask_img, mask_uri, _) in upload_pairs:
                    tasks.append(_upload_detection_artifacts(crop_img, crop_uri, mask_img, mask_uri))

                await asyncio.gather(*tasks)
                update_frame_record(frame_id, {"status": "segmented"})

                for crop_img, crop_uri, mask_img, mask_uri, det in upload_pairs:
                    detection_id = det["detection_id"]

                    detection_rows.append({
                        "id": detection_id,
                        "frame_id": frame_id,
                        "project_id": project_id,
                        "upload_id": upload_id,
                        "label_id": det.get("label_id"),
                        "display_label": det.get("display_label"),
                        "prompt": det.get("prompt"),
                        "bbox": det["bbox"],
                        "score": det["score"],
                        "blur_score": det.get("blur_score"),
                        "crop_gcs_uri": crop_uri,
                        "mask_gcs_uri": mask_uri,
                        "status": "needs_review",
                    })

                    if det.get("crop_embedding"):
                        detection_embedding_rows.append({
                            "id": detection_id,
                            "frame_id": frame_id,
                            "project_id": project_id,
                            "upload_id": upload_id,
                            "crop_gcs_uri": crop_uri,
                            "embedding": det["crop_embedding"],
                        })

                if frame_result.get("frame_embedding"):
                    frame_embedding_rows.append({
                        "id": frame_id,
                        "project_id": project_id,
                        "upload_id": upload_id,
                        "frame_gcs_uri": frame_gcs_uri,
                        "embedding": frame_result["frame_embedding"],
                    })

            insert_detection_records(detection_rows)

            if frame_embedding_rows:
                upsert_frame_embeddings(frame_embedding_rows)

            if detection_embedding_rows:
                upsert_detection_embeddings(detection_embedding_rows)

            frames_processed += len(chunk_records)
            update_upload_record(upload_id, {
                "frames_processed": frames_processed,
            })

        update_upload_record(upload_id, {"status": "ready"})

    except asyncio.CancelledError:
        update_upload_record(upload_id, {
            "status": "failed",
            "error_message": "Processing was interrupted (server restart or timeout)",
        })
        raise
    except Exception as exc:
        update_upload_record(upload_id, {
            "status": "failed",
            "error_message": str(exc),
        })
        import traceback
        traceback.print_exc()
