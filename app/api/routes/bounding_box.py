from io import BytesIO
from typing import List, Optional
from datetime import datetime
import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.concurrency import run_in_threadpool
from PIL import Image
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.api.helper.embed import upsert_clip_detection_embeddings, upsert_detection_embeddings
from app.services.gcp_storage import download_bytes_from_gcs
from app.services.model_service import embed_crop_image, embed_crop_image_dino
from app.services.supabase_service import get_supabase_client

router = APIRouter()
supabase = get_supabase_client()


# ─────────────────────────────────────────────
#  SCHEMAS
# ─────────────────────────────────────────────

class BoundingBoxCreate(BaseModel):
    bbox: list[float] = Field(..., min_length=4, max_length=4, description="[x1, y1, x2, y2] in pixels")
    display_label: str
    score: Optional[float] = Field(None, ge=0.0, le=1.0)
    notes: Optional[str] = None


class BoundingBoxUpdate(BaseModel):
    bbox: Optional[list[float]] = Field(None, min_length=4, max_length=4)
    display_label: Optional[str] = None
    score: Optional[float] = Field(None, ge=0.0, le=1.0)
    status: Optional[str] = None
    notes: Optional[str] = None


class BoundingBoxResponse(BaseModel):
    id: str
    frame_id: str
    project_id: str
    upload_id: Optional[str]
    display_label: str
    bbox: list[float]
    score: Optional[float]
    status: Optional[str]
    annotation_source: str          # "machine" | "human"
    created_at: datetime


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def _verify_project_ownership(project_id: str, user_id: str):
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


def _verify_frame_exists(frame_id: str, project_id: str):
    result = (
        supabase
        .table("frames")
        .select("id, upload_id")
        .eq("id", frame_id)
        .eq("project_id", project_id)
        .execute()
    )
    if not result.data:
        raise HTTPException(status_code=404, detail="Frame not found")
    return result.data[0]

def _derive_label_id(display_label: str) -> str:
    return display_label.strip().lower().replace(" ", "_")

# ─────────────────────────────────────────────
#  ROUTES
# ─────────────────────────────────────────────

@router.post(
    "/projects/{project_id}/frames/{frame_id}/bounding-boxes",
    response_model=BoundingBoxResponse,
    status_code=status.HTTP_201_CREATED
)
async def create_bounding_box(
    project_id: str,
    frame_id: str,
    bbox: BoundingBoxCreate,
    user_id: str = Depends(get_current_user)
):
    _verify_project_ownership(project_id, user_id)
    frame = _verify_frame_exists(frame_id, project_id)

    payload = {
        "id": str(uuid.uuid4()),
        "frame_id": frame_id,
        "project_id": project_id,
        "upload_id": frame.get("upload_id"),
        "display_label": bbox.display_label,
        "label_id": _derive_label_id(bbox.display_label),  
        "bbox": bbox.bbox,
        "score": bbox.score if bbox.score is not None else 0.0,
        "crop_gcs_uri": "",
        "status": "reviewed", 
        "annotation_source": "human",
    }

    try:
        result = supabase.table("detections").insert(payload).execute()
        if not result.data:
            raise HTTPException(status_code=500, detail="Failed to create bounding box")
        detection = result.data[0]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    # Generate CLIP embedding for the crop so it appears in semantic search
    try:
        frame_res = (
            supabase.table("frames")
            .select("frame_gcs_uri")
            .eq("id", frame_id)
            .single()
            .execute()
        )
        frame_gcs_uri = frame_res.data["frame_gcs_uri"] if frame_res.data else None

        if frame_gcs_uri:
            frame_bytes = await run_in_threadpool(download_bytes_from_gcs, frame_gcs_uri)
            image = Image.open(BytesIO(frame_bytes)).convert("RGB")
            x1, y1, x2, y2 = [int(v) for v in bbox.bbox]
            crop = image.crop((x1, y1, x2, y2))
            buf = BytesIO()
            crop.convert("RGB").save(buf, format="JPEG", quality=90)
            crop_bytes = buf.getvalue()

            clip_embedding = await embed_crop_image(crop_bytes)
            upsert_clip_detection_embeddings([{
                "id": detection["id"],
                "frame_id": frame_id,
                "project_id": project_id,
                "upload_id": frame.get("upload_id"),
                "crop_gcs_uri": "",
                "embedding": clip_embedding,
            }])

            dino_embedding = await embed_crop_image_dino(crop_bytes)
            upsert_detection_embeddings([{
                "id": detection["id"],
                "frame_id": frame_id,
                "project_id": project_id,
                "upload_id": frame.get("upload_id"),
                "crop_gcs_uri": "",
                "embedding": dino_embedding,
            }])
    except Exception:
        pass  

    return detection


@router.get(
    "/projects/{project_id}/frames/{frame_id}/bounding-boxes",
    response_model=List[BoundingBoxResponse]
)
async def get_bounding_boxes(
    project_id: str,
    frame_id: str,
    user_id: str = Depends(get_current_user)
):
    _verify_project_ownership(project_id, user_id)
    _verify_frame_exists(frame_id, project_id)

    try:
        result = (
            supabase
            .table("detections")
            .select("*")
            .eq("frame_id", frame_id)
            .eq("project_id", project_id)
            .eq("annotation_source", "human")   # only return human-drawn ones
            .execute()
        )
        return result.data or []

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get(
    "/projects/{project_id}/frames/{frame_id}/bounding-boxes/{bbox_id}",
    response_model=BoundingBoxResponse
)
async def get_bounding_box(
    project_id: str,
    frame_id: str,
    bbox_id: str,
    user_id: str = Depends(get_current_user)
):
    _verify_project_ownership(project_id, user_id)

    try:
        result = (
            supabase
            .table("detections")
            .select("*")
            .eq("id", bbox_id)
            .eq("frame_id", frame_id)
            .eq("project_id", project_id)
            .eq("annotation_source", "human")
            .execute()
        )
        if not result.data:
            raise HTTPException(status_code=404, detail="Bounding box not found")
        return result.data[0]

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

@router.put(
    "/projects/{project_id}/frames/{frame_id}/bounding-boxes/{bbox_id}",
    response_model=BoundingBoxResponse
)
async def update_bounding_box(
    project_id: str,
    frame_id: str,
    bbox_id: str,
    bbox: BoundingBoxUpdate,
    user_id: str = Depends(get_current_user)
):
    _verify_project_ownership(project_id, user_id)

    update_data = bbox.model_dump(exclude_unset=True)
    
    # only keep fields that exist in the detections table
    allowed_fields = {"bbox", "display_label", "score", "status"}
    update_data = {k: v for k, v in update_data.items() if k in allowed_fields}
    
    # also update label_id if display_label is being changed
    if "display_label" in update_data:
        update_data["label_id"] = _derive_label_id(update_data["display_label"])

    if not update_data:
        raise HTTPException(status_code=400, detail="No data to update")

    try:
        result = (
            supabase
            .table("detections")
            .update(update_data)
            .eq("id", bbox_id)
            .eq("frame_id", frame_id)
            .eq("project_id", project_id)
            .eq("annotation_source", "human")
            .execute()
        )
        if not result.data:
            raise HTTPException(status_code=404, detail="Bounding box not found")
        return result.data[0]

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

@router.delete(
    "/projects/{project_id}/frames/{frame_id}/bounding-boxes/{bbox_id}",
    status_code=status.HTTP_204_NO_CONTENT
)
async def delete_bounding_box(
    project_id: str,
    frame_id: str,
    bbox_id: str,
    user_id: str = Depends(get_current_user)
):
    _verify_project_ownership(project_id, user_id)

    try:
        result = (
            supabase
            .table("detections")
            .delete()
            .eq("id", bbox_id)
            .eq("frame_id", frame_id)
            .eq("project_id", project_id)
            .eq("annotation_source", "human")   # safety: can only delete human rows
            .execute()
        )
        if not result.data:
            raise HTTPException(status_code=404, detail="Bounding box not found")

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))