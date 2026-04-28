from fastapi import APIRouter, Depends, HTTPException

from app.auth import get_current_user
from app.services.supabase_service import get_supabase_client

router = APIRouter()
supabase = get_supabase_client()


@router.get("/uploads/{upload_id}/status")
async def get_upload_status(
    upload_id: str,
    user_id: str = Depends(get_current_user),
):
    upload_result = (
        supabase
        .table("uploads")
        .select("status,frame_count,frames_processed,error_message")
        .eq("id", upload_id)
        .execute()
    )

    if not upload_result.data:
        raise HTTPException(status_code=404, detail="Upload not found")

    upload = upload_result.data[0]

    det_result = (
        supabase
        .table("detections")
        .select("id", count="exact")
        .eq("upload_id", upload_id)
        .execute()
    )
    detections_found = det_result.count or 0

    response = {
        "status": upload["status"],
        "total_frames": upload.get("frame_count") or 0,
        "frames_processed": upload.get("frames_processed") or 0,
        "detections_found": detections_found,
    }

    if upload.get("status") == "failed" and upload.get("error_message"):
        response["error_message"] = upload["error_message"]

    return response
