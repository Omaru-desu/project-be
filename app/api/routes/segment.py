from fastapi import APIRouter, Depends, HTTPException

from app.auth import get_current_user
from app.api.helper.upload import (
    get_active_label_ids,
    get_frames_for_upload,
    get_project_for_user,
    insert_detection_records,
    update_upload_record,
)
from app.services.model_service import segment_frames

router = APIRouter()


@router.post("/projects/{project_id}/uploads/{upload_id}/segment")
def segment_upload_frames(
    project_id: str,
    upload_id: str,
    user_id: str = Depends(get_current_user),
):
    get_project_for_user(project_id, user_id)

    frames = get_frames_for_upload(upload_id)
    if not frames:
        raise HTTPException(status_code=404, detail="No frames found for this upload")

    label_ids = get_active_label_ids(project_id)

    segment_requests = [
        {
            "frame_id": f["id"],
            "project_id": project_id,
            "upload_id": upload_id,
            "frame_gcs_uri": f["frame_gcs_uri"],
        }
        for f in frames
    ]

    results = segment_frames(segment_requests, label_ids=label_ids)

    detection_rows = []
    total_detections = 0

    for result in results:
        frame_id = result["frame_id"]
        for det in result["detections"]:
            detection_rows.append({
                "id": det["detection_id"],
                "frame_id": frame_id,
                "project_id": project_id,
                "upload_id": upload_id,
                "label_id": det.get("label_id"),
                "display_label": det.get("display_label"),
                "prompt": det.get("prompt"),
                "bbox": det["bbox"],
                "score": det["score"],
                "blur_score": det.get("blur_score"),
                "crop_gcs_uri": det["crop_gcs_uri"],
                "mask_gcs_uri": det.get("mask_gcs_uri"),
            })
        total_detections += len(result["detections"])

    insert_detection_records(detection_rows)

    update_upload_record(upload_id, {"status": "segmented"})

    return {
        "upload_id": upload_id,
        "frame_count": len(results),
        "detection_count": total_detections,
        "label_ids_used": label_ids,
        "status": "segmented",
    }