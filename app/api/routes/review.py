from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.auth import get_current_user
from app.services.supabase_service import get_supabase_client

router = APIRouter()
supabase = get_supabase_client()


class ReviewDetectionLabel(BaseModel):
    label_id: str
    display_label: str


@router.patch("/detections/{detection_id}/label")
def review_detection_label(
    detection_id: str,
    body: ReviewDetectionLabel,
    user_id: str = Depends(get_current_user),
):
    det_res = (
        supabase
        .table("detections")
        .select("id, project_id")
        .eq("id", detection_id)
        .single()
        .execute()
    )

    if not det_res.data:
        raise HTTPException(status_code=404, detail="Detection not found")

    project_id = det_res.data["project_id"]

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

    try:
        update_res = (
            supabase
            .table("detections")
            .update({
                "label_id": body.label_id,
                "display_label": body.display_label,
                "status": "reviewed",
            })
            .eq("id", detection_id)
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to update detection: {exc}") from exc

    return update_res.data[0] if update_res.data else {}
