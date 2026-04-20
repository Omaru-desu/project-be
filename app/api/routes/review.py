from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.auth import get_current_user
from app.services.supabase_service import get_supabase_client

router = APIRouter()
supabase = get_supabase_client()


class ReviewDetectionLabel(BaseModel):
    display_label: str


def _derive_label_id(display_label: str) -> str:
    return display_label.strip().lower().replace(" ", "_")


def _get_detection_and_verify_owner(detection_id: str, user_id: str) -> dict:
    det_res = (
        supabase
        .table("detections")
        .select("*")
        .eq("id", detection_id)
        .single()
        .execute()
    )

    if not det_res.data:
        raise HTTPException(status_code=404, detail="Detection not found")

    det = det_res.data
    project_id = det["project_id"]

    proj_res = (
        supabase
        .table("projects")
        .select("id")
        .eq("id", project_id)
        .eq("owner", user_id)
        .single()
        .execute()
    )

    if not proj_res.data:
        raise HTTPException(status_code=403, detail="Access denied")

    return det


@router.get("/detections/{detection_id}/review")
def get_detection_for_review(
    detection_id: str,
    user_id: str = Depends(get_current_user),
):
    det = _get_detection_and_verify_owner(detection_id, user_id)

    frame_res = (
        supabase
        .table("frames")
        .select("frame_gcs_uri")
        .eq("id", det["frame_id"])
        .single()
        .execute()
    )
    frame_gcs_uri = frame_res.data["frame_gcs_uri"] if frame_res.data else None

    return {
        "id": det["id"],
        "frame_id": det["frame_id"],
        "project_id": det["project_id"],
        "display_label": det["display_label"],
        "label_id": det["label_id"],
        "status": det["status"],
        "bbox": det["bbox"],
        "score": det.get("score"),
        "blur_score": det.get("blur_score"),
        "crop_gcs_uri": det.get("crop_gcs_uri"),
        "frame_gcs_uri": frame_gcs_uri,
    }


@router.patch("/detections/{detection_id}/label")
def review_detection_label(
    detection_id: str,
    body: ReviewDetectionLabel,
    user_id: str = Depends(get_current_user),
):
    _get_detection_and_verify_owner(detection_id, user_id)

    derived_label_id = _derive_label_id(body.display_label)

    try:
        update_res = (
            supabase
            .table("detections")
            .update({
                "label_id": derived_label_id,
                "display_label": body.display_label,
                "status": "reviewed",
            })
            .eq("id", detection_id)
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to update detection: {exc}") from exc

    return update_res.data[0] if update_res.data else {}
