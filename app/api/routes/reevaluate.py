import uuid
from fastapi import APIRouter, Depends, HTTPException
from app.auth import get_current_user
from app.api.helper.segment import insert_detection_records
from app.api.helper.upload import get_project_for_user
from app.services.model_service import segment_frame_with_prompt
from app.services.supabase_service import get_supabase_client
from pydantic import BaseModel
from app.services.gcp_storage import download_bytes_from_gcs

router = APIRouter()
supabase = get_supabase_client()

IOU_THRESHOLD = 0.5

class ReevaluateRequest(BaseModel):
    prompt: str

def _iou(a: list[float], b: list[float]) -> float:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    inter_x1 = max(ax1, bx1)
    inter_y1 = max(ay1, by1)
    inter_x2 = min(ax2, bx2)
    inter_y2 = min(ay2, by2)
    inter_w = max(0.0, inter_x2 - inter_x1)
    inter_h = max(0.0, inter_y2 - inter_y1)
    inter_area = inter_w * inter_h
    a_area = (ax2 - ax1) * (ay2 - ay1)
    b_area = (bx2 - bx1) * (by2 - by1)
    union_area = a_area + b_area - inter_area
    if union_area <= 0:
        return 0.0
    return inter_area / union_area


@router.post("/projects/{project_id}/frames/{frame_id}/reevaluate")
async def reevaluate_frame(
    project_id: str,
    frame_id: str,
    request: ReevaluateRequest,
    user_id: str = Depends(get_current_user),
):
    # 1. Verify ownership
    get_project_for_user(project_id, user_id)

    # 2. Get frame
    frame_res = (
        supabase
        .table("frames")
        .select("*")
        .eq("id", frame_id)
        .eq("project_id", project_id)
        .single()
        .execute()
    )
    if not frame_res.data:
        raise HTTPException(status_code=404, detail="Frame not found")

    frame = frame_res.data
    frame_gcs_uri = frame["frame_gcs_uri"]
    upload_id = frame["upload_id"]

    # 3. Run SAM3 with user prompt
    try:
        sam_result = await segment_frame_with_prompt(
            image_bytes = download_bytes_from_gcs(frame_gcs_uri),
            prompt=request.prompt,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"SAM3 failed: {exc}")

    boxes = sam_result.get("boxes", [])
    scores = sam_result.get("scores", [])

    new_detections = []

    for bbox, score in zip(boxes, scores):
        new_detections.append({
            "bbox": bbox,
            "score": score,
            "prompt": request.prompt,
            "display_label": request.prompt,
            "label_id": None,
        })

    # 5. Get existing detections for IoU comparison
    existing_res = (
        supabase
        .table("detections")
        .select("id, bbox")
        .eq("frame_id", frame_id)
        .execute()
    )
    existing_bboxes = [d["bbox"] for d in (existing_res.data or [])]

    # 6. Filter out detections that overlap with existing ones
    non_overlapping = []
    for det in new_detections:
        new_bbox = det["bbox"]
        overlaps = any(
            _iou(new_bbox, existing_bbox) >= IOU_THRESHOLD
            for existing_bbox in existing_bboxes
        )
        if not overlaps:
            non_overlapping.append(det)

    if not non_overlapping:
        return {
            "frame_id": frame_id,
            "new_detections": 0,
            "skipped_duplicates": len(new_detections),
            "message": "No new detections — all overlapped with existing ones",
        }

    # 7. Save non-overlapping detections
    detection_rows = []
    for det in non_overlapping:
        detection_id = det.get("detection_id") or str(uuid.uuid4())
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
            "crop_gcs_uri": det.get("crop_gcs_uri", ""),   # ← add empty string fallback
            "mask_gcs_uri": det.get("mask_gcs_uri", ""), 
            "status": "needs_review",
            "annotation_source": "machine",
        })

    insert_detection_records(detection_rows)

    return {
        "frame_id": frame_id,
        "new_detections": len(detection_rows),
        "skipped_duplicates": len(new_detections) - len(non_overlapping),
        "detection_ids": [d["id"] for d in detection_rows],
    }