import asyncio
import base64
import os
import tempfile
 
from app.api.helper.upload import update_upload_record, update_frame_record, insert_frame_records
from app.api.helper.segment import insert_detection_records
from app.api.helper.embed import upsert_detection_embeddings, upsert_frame_embeddings, upsert_clip_detection_embeddings
from app.services.gcp_storage import (
    upload_bytes_to_gcs_async,
    build_detection_artifact_gcs_uris,
)
from app.services.model_service import process_frames_deim as call_model_process_frames
from app.services.supabase_service import get_supabase_client
from app.services.rosbag_processor import extract_rosbag_frames
 
 
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
 
 
def _get_project_model(project_id: str) -> dict | None:
    supabase = get_supabase_client()
    res = (
        supabase
        .table("project_models")
        .select("*")
        .eq("project_id", project_id)
        .single()
        .execute()
    )
    return res.data
 
 
async def process_upload(
    upload_id: str,
    project_id: str,
    user_id: str,
    frame_records: list[dict],
    label_ids: list[str] | None,
    frame_bytes_map: dict[str, bytes],
    upload_type: str = "",
    rosbag_gcs_uri: str | None = None,   # GCS URI of the raw .bag/.db3 file
    bucket_name: str | None = None,       # bucket name for building frame GCS paths
) -> None:
    try:
        # ── ROSBAG: download from GCS, extract frames, then process ──────────
        if upload_type == "rosbag" and rosbag_gcs_uri and bucket_name:
            update_upload_record(upload_id, {"status": "processing_frames"})
 
            from app.services.gcp_storage import download_bytes_from_gcs
 
            # Download the raw bag from GCS into a temp file
            with tempfile.TemporaryDirectory() as temp_dir:
                # Derive filename from GCS URI (last path segment)
                bag_filename = rosbag_gcs_uri.split("/")[-1]
                bag_path = os.path.join(temp_dir, bag_filename)
 
                # Download bag from GCS
                bag_bytes = download_bytes_from_gcs(rosbag_gcs_uri)
                with open(bag_path, "wb") as f:
                    f.write(bag_bytes)
                del bag_bytes  # free memory
 
                # Extract frames from the bag
                extract_dir = os.path.join(temp_dir, "frames")
                os.makedirs(extract_dir, exist_ok=True)
                extracted = extract_rosbag_frames(bag_path, extract_dir, frame_skip=10)
 
                if not extracted:
                    update_upload_record(upload_id, {
                        "status": "failed",
                        "error_message": "No image frames found in rosbag file",
                    })
                    return
 
                # Build frame_records and frame_bytes_map
                for idx, frame in enumerate(extracted):
                    with open(frame["local_path"], "rb") as f:
                        frame_bytes_map[f"{upload_id}_{idx:06d}"] = f.read()
 
                    frame_id = f"{upload_id}_{idx:06d}"
                    frame_filename = f"frame_{idx:06d}.jpg"
                    frame_gcs_path = (
                        f"projects/{project_id}/uploads/{upload_id}/frames/{frame_filename}"
                    )
 
                    frame_records.append({
                        "id": frame_id,
                        "source_filename": frame["frame_filename"],
                        "frame_gcs_uri": f"gs://{bucket_name}/{frame_gcs_path}",
                        "status": "queued",
                        "owner": user_id,
                        "upload_id": upload_id,
                        "project_id": project_id,
                    })
 
                insert_frame_records(frame_records)
                update_upload_record(upload_id, {
                    "frame_count": len(frame_records),
                    "status": "segmenting",
                })
 
        # ── Shared processing logic (image / video / rosbag) ─────────────────
        update_upload_record(upload_id, {"status": "segmenting"})
 
        chunk_size = 50
        total_frames = len(frame_records)
        total_chunks = (total_frames + chunk_size - 1) // chunk_size
        frames_processed = 0
        upload_tasks: list[asyncio.Task] = []
        uploaded_frame_ids: list[str] = []
 
        project_model = _get_project_model(project_id)
        is_custom_untrained = (
            project_model is not None and
            project_model["model_type"] == "custom" and
            project_model["checkpoint_url"] is None
        )
 
        for chunk_num, i in enumerate(range(0, total_frames, chunk_size)):
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
 
            if is_custom_untrained:
                model_results = [
                    {"frame_id": f["id"], "detections": [], "frame_embedding": None, "clip_frame_embedding": None}
                    for f in chunk_records
                ]
            else:
                is_final_chunk = (chunk_num == total_chunks - 1)
                model_results = await call_model_process_frames(
                    chunk_bytes_map,
                    chunk_metadata,
                    label_ids,
                    upload_id=upload_id,
                    upload_type=upload_type,
                    is_final_chunk=is_final_chunk,
                )
 
            detection_rows: list[dict] = []
            frame_embedding_rows: list[dict] = []
            detection_embedding_rows: list[dict] = []
            per_frame_artifacts: list[tuple[str, bytes, str, list[tuple[str, str, str, str]]]] = []
            clip_embedding_rows: list[dict] = []
 
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
                        "track_id": det.get("track_id"),
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
 
                    if det.get("clip_embedding") is not None:
                        clip_embedding_rows.append({
                            "id": detection_id,
                            "frame_id": frame_id,
                            "project_id": project_id,
                            "upload_id": upload_id,
                            "crop_gcs_uri": crop_gcs_uri,
                            "embedding": det["clip_embedding"],
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
 
            if clip_embedding_rows:
                upsert_clip_detection_embeddings(clip_embedding_rows)
 
            frames_processed += len(chunk_records)
            update_upload_record(upload_id, {
                "frames_processed": frames_processed,
            })
 
            for frame_id, frame_bytes, frame_gcs_uri, detection_artifacts in per_frame_artifacts:
                uploaded_frame_ids.append(frame_id)
                upload_tasks.append(asyncio.create_task(
                    _upload_frame_artifacts(frame_id, frame_bytes, frame_gcs_uri, detection_artifacts)
                ))
 
        if upload_tasks:
            await asyncio.gather(*upload_tasks)
 
        for frame_id in uploaded_frame_ids:
            update_frame_record(frame_id, {"status": "segmented"})
 
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
