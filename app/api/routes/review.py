from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.auth import get_current_user
from app.services.supabase_service import get_supabase_client

from app.services import model_service
import asyncio

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
async def review_detection_label(
    detection_id: str,
    body: ReviewDetectionLabel,
    user_id: str = Depends(get_current_user),
):
    det = _get_detection_and_verify_owner(detection_id, user_id)
    project_id = det["project_id"]
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

    try:
        if project_id:
            supabase.table("projects").update({"updated_at": datetime.now(timezone.utc).isoformat()}).eq("id", project_id).execute()
    except Exception:
        pass

    return update_res.data[0] if update_res.data else {}

@router.delete("/detections/{detection_id}")
def delete_detection(
    detection_id: str,
    user_id: str = Depends(get_current_user),
):
    _get_detection_and_verify_owner(detection_id, user_id)

    try:
        supabase.table("detections").update({"status": "deleted"}).eq("id", detection_id).execute()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to delete detection: {exc}") from exc

    return {"success": True}

@router.post("/projects/{project_id}/frames/{frame_id}/approve")
async def approve_frame(
    project_id: str,
    frame_id: str,
    user_id: str = Depends(get_current_user),
):
    # verify ownership
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

    # bulk update all unreviewed detections in this frame to reviewed
    supabase.table("detections").update({
        "status": "reviewed"
    }).eq("frame_id", frame_id).eq("status", "needs_review").execute()

    # increment approved frame counter in project_models
    model_res = (
        supabase
        .table("project_models")
        .select("*")
        .eq("project_id", project_id)
        .single()
        .execute()
    )

    did_retrain = False
    if model_res.data:
        model_row = model_res.data
        current_count = model_row["approved_since_last_retrain"] + 1
        did_retrain = current_count >= 10

        if did_retrain:
            supabase.table("project_models").update({
                "approved_since_last_retrain": 0
            }).eq("id", model_row["id"]).execute()

            # fetch latest reviewed detections to use as training data
            reviewed_res = (
                supabase
                .table("detections")
                .select("*, frames(frame_url)")
                .eq("project_id", project_id)
                .eq("status", "reviewed")
                .order("updated_at", desc=True)
                .execute()
            )
            annotations = [
                {
                    "frame_url": d["frames"]["frame_url"],
                    "bbox": d["bbox"],
                    "label": d["display_label"],
                }
                for d in (reviewed_res.data or [])
                if d.get("frames")
            ]
            asyncio.create_task(
                model_service.retrain_project(project_id, annotations)
            )
        else:
            supabase.table("project_models").update({
                "approved_since_last_retrain": current_count
            }).eq("id", model_row["id"]).execute()
        
    supabase.table("frames").update({
        "is_approved": True
    }).eq("id", frame_id).execute()

    return {"retrained": did_retrain}