import asyncio
import base64

from app.api.helper.upload import update_upload_record, update_frame_record
from app.api.helper.segment import insert_detection_records
from app.api.helper.embed import upsert_detection_embeddings, upsert_frame_embeddings
from app.services.gcp_storage import (
    upload_bytes_to_gcs_async,
    build_detection_artifact_gcs_uris,
)
from app.services.model_service import process_frames_deim as call_model_process_frames


async def _upload_frame_artifacts(
    frame_id: str,
    frame_bytes: bytes,
    frame_gcs_uri: str,
    detection_artifacts: list[tuple[str, str, str, str]],
) -> None:
    tasks = [upload_bytes_to_gcs_async(frame_bytes, frame_gcs_uri, "image/jpeg")]
    for crop_b64, crop_uri, mask_b64, mask_uri in detection_artifacts:
        tasks.append(upload_bytes_to_gcs_async(base64.b64decode(crop_b64), crop_uri, "image/jpeg"))
        tasks.append(upload_bytes_to_gcs_async(base64.b64decode(mask_b64), mask_uri, "image/png"))
    await asyncio.gather(*tasks)
    update_frame_record(frame_id, {"status": "segmented"})


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

        chunk_size = 50
        total_frames = len(frame_records)
        frames_processed = 0
        upload_tasks: list[asyncio.Task] = []

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
            per_frame_artifacts: list[tuple[str, bytes, str, list[tuple[str, str, str, str]]]] = []

            for frame_result in model_results:
                frame_id = frame_result["frame_id"]
                frame_gcs_uri = chunk_uri_map.get(frame_id, "")
                frame_detections = frame_result.get("detections", [])

                detection_artifacts: list[tuple[str, str, str, str]] = []
                for det in frame_detections:
                    detection_id = det["detection_id"]

                    crop_gcs_uri, mask_gcs_uri = build_detection_artifact_gcs_uris(
                        frame_gcs_uri=frame_gcs_uri,
                        detection_id=detection_id,
                    )

                    detection_artifacts.append(
                        (det["crop_image"], crop_gcs_uri, det["mask_image"], mask_gcs_uri)
                    )

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
                        "crop_gcs_uri": crop_gcs_uri,
                        "mask_gcs_uri": mask_gcs_uri,
                        "status": "needs_review",
                    })

                    if det.get("crop_embedding"):
                        detection_embedding_rows.append({
                            "id": detection_id,
                            "frame_id": frame_id,
                            "project_id": project_id,
                            "upload_id": upload_id,
                            "crop_gcs_uri": crop_gcs_uri,
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

                per_frame_artifacts.append(
                    (frame_id, chunk_bytes_map[frame_id], frame_gcs_uri, detection_artifacts)
                )

            insert_detection_records(detection_rows)

            if frame_embedding_rows:
                upsert_frame_embeddings(frame_embedding_rows)

            if detection_embedding_rows:
                upsert_detection_embeddings(detection_embedding_rows)

            frames_processed += len(chunk_records)
            update_upload_record(upload_id, {
                "frames_processed": frames_processed,
            })

            for frame_id, frame_bytes, frame_gcs_uri, detection_artifacts in per_frame_artifacts:
                upload_tasks.append(asyncio.create_task(
                    _upload_frame_artifacts(frame_id, frame_bytes, frame_gcs_uri, detection_artifacts)
                ))


        if upload_tasks:
            await asyncio.gather(*upload_tasks)

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
