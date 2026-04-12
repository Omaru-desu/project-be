from fastapi import APIRouter, Depends, HTTPException

from app.auth import get_current_user
from app.api.helper.upload import (
    get_frames_for_upload,
    get_project_for_user,
    update_upload_record,
)
from app.services.model_service import embed_frames
from app.api.helper.embed import get_detections_for_upload, upsert_detection_embeddings, upsert_frame_embeddings

router = APIRouter()


@router.post("/projects/{project_id}/uploads/{upload_id}/embed")
def embed_upload_frames(
    project_id: str,
    upload_id: str,
    user_id: str = Depends(get_current_user),
):
    get_project_for_user(project_id, user_id)

    frames = get_frames_for_upload(upload_id)
    if not frames:
        raise HTTPException(status_code=404, detail="No frames found for this upload")

    embed_requests = [
        {
            "frame_id": f["id"],
            "frame_gcs_uri": f["frame_gcs_uri"],
            "detections": [],
        }
        for f in frames
    ]

    results = embed_frames(embed_requests)

    frame_lookup = {f["id"]: f for f in frames}
    embedding_rows = [
        {
            "id": r["frame_id"],
            "project_id": project_id,
            "upload_id": upload_id,
            "frame_gcs_uri": frame_lookup[r["frame_id"]]["frame_gcs_uri"],
            "embedding": r["frame_embedding"],
        }
        for r in results
    ]

    upsert_frame_embeddings(embedding_rows)

    update_upload_record(upload_id, {"status": "embedded"})

    return {
        "upload_id": upload_id,
        "embedded_count": len(embedding_rows),
        "status": "embedded",
    }


@router.post("/projects/{project_id}/uploads/{upload_id}/embed/detections")
def embed_upload_detections(
    project_id: str,
    upload_id: str,
    user_id: str = Depends(get_current_user),
):
    get_project_for_user(project_id, user_id)

    detections = get_detections_for_upload(upload_id)
    if not detections:
        raise HTTPException(status_code=404, detail="No detections found — run /segment first")

    frames_map: dict[str, dict] = {}
    for det in detections:
        fid = det["frame_id"]
        if fid not in frames_map:
            frames_map[fid] = {
                "frame_id": fid,
                "frame_gcs_uri": "", 
                "detections": [],
            }
        frames_map[fid]["detections"].append({
            "detection_id": det["id"],
            "crop_gcs_uri": det["crop_gcs_uri"],
        })

    results = embed_frames(list(frames_map.values()))

    det_lookup = {d["id"]: d for d in detections}
    embedding_rows = []
    for result in results:
        for det_result in result["detections"]:
            det_id = det_result["detection_id"]
            det = det_lookup[det_id]
            embedding_rows.append({
                "id": det_id,
                "frame_id": det["frame_id"],
                "project_id": project_id,
                "upload_id": upload_id,
                "crop_gcs_uri": det["crop_gcs_uri"],
                "embedding": det_result["crop_embedding"],
            })

    upsert_detection_embeddings(embedding_rows)

    return {
        "upload_id": upload_id,
        "embedded_count": len(embedding_rows),
        "status": "detections_embedded",
    }
