from typing import Annotated, Literal, Optional, Union
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.services.supabase_service import get_supabase_client

router = APIRouter()
supabase = get_supabase_client()


# ─────────────────────────────────────────────
#  REQUEST/RESPONSE SCHEMAS
# ─────────────────────────────────────────────

class AssignTrackBody(BaseModel):
    action: Literal["assign"]
    track_id: str


class CreateTrackBody(BaseModel):
    action: Literal["create"]


class RemoveTrackBody(BaseModel):
    action: Literal["remove"]


TrackEditBody = Annotated[
    Union[AssignTrackBody, CreateTrackBody, RemoveTrackBody],
    Field(discriminator="action"),
]


class TrackEditResponse(BaseModel):
    detection_id: str
    track_id: Optional[str]
    previous_track_id: Optional[str]


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def _verify_project_ownership(project_id: str, user_id: str) -> None:
    result = (
        supabase
        .table("projects")
        .select("id")
        .eq("id", project_id)
        .eq("owner", user_id)
        .execute()
    )
    if not result.data:
        raise HTTPException(status_code=404, detail="Project not found")


def _fetch_detection(project_id: str, detection_id: str) -> dict:
    result = (
        supabase
        .table("detections")
        .select("id, project_id, upload_id, track_id")
        .eq("id", detection_id)
        .eq("project_id", project_id)
        .execute()
    )
    if not result.data:
        raise HTTPException(status_code=404, detail="Detection not found")
    return result.data[0]


def _track_exists_in_upload(project_id: str, upload_id: str, track_id: str) -> bool:
    result = (
        supabase
        .table("detections")
        .select("id")
        .eq("project_id", project_id)
        .eq("upload_id", upload_id)
        .eq("track_id", track_id)
        .limit(1)
        .execute()
    )
    return bool(result.data)


# ─────────────────────────────────────────────
#  ROUTES
# ─────────────────────────────────────────────

@router.patch(
    "/projects/{project_id}/detections/{detection_id}/track",
    response_model=TrackEditResponse,
)
def edit_detection_track(
    project_id: str,
    detection_id: str,
    body: TrackEditBody,
    user_id: str = Depends(get_current_user),
):
    _verify_project_ownership(project_id, user_id)
    detection = _fetch_detection(project_id, detection_id)

    previous_track_id: Optional[str] = detection.get("track_id")
    upload_id: str = detection["upload_id"]

    if body.action == "assign":
        if not _track_exists_in_upload(project_id, upload_id, body.track_id):
            raise HTTPException(
                status_code=400,
                detail="track_id not found in this upload",
            )
        new_track_id: Optional[str] = body.track_id
    elif body.action == "create":
        new_track_id = uuid4().hex
    elif body.action == "remove":
        new_track_id = None
    else:
        raise HTTPException(status_code=400, detail="Unknown action")

    update_res = (
        supabase
        .table("detections")
        .update({"track_id": new_track_id})
        .eq("id", detection_id)
        .eq("project_id", project_id)
        .execute()
    )
    if not update_res.data:
        raise HTTPException(status_code=500, detail="Failed to update detection")

    return TrackEditResponse(
        detection_id=detection_id,
        track_id=new_track_id,
        previous_track_id=previous_track_id,
    )
