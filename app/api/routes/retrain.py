from fastapi import APIRouter, Depends, HTTPException

from app.auth import get_current_user
from app.services import model_service
from app.services.supabase_service import get_supabase_client

router = APIRouter()
supabase = get_supabase_client()


@router.get("/projects/{project_id}/retrain/status")
async def get_retrain_status(
    project_id: str,
    user_id: str = Depends(get_current_user),
):
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

    model_res = (
        supabase
        .table("project_models")
        .select(
            "retrain_job_id, retrain_status, retrain_error, last_retrain_at, "
            "checkpoint_url, model_type, approved_since_last_retrain"
        )
        .eq("project_id", project_id)
        .single()
        .execute()
    )

    if not model_res.data:
        raise HTTPException(status_code=404, detail="project_models row not found")

    row = model_res.data
    response = {
        "project_id": project_id,
        "model_type": row.get("model_type"),
        "job_id": row.get("retrain_job_id"),
        "status": row.get("retrain_status"),
        "error": row.get("retrain_error"),
        "last_retrain_at": row.get("last_retrain_at"),
        "checkpoint_url": row.get("checkpoint_url"),
        "approved_since_last_retrain": row.get("approved_since_last_retrain"),
    }

    if row.get("retrain_job_id") and row.get("retrain_status") in ("queued", "training"):
        try:
            live = await model_service.get_retrain_job(row["retrain_job_id"])
            response["live"] = live
        except Exception as exc:
            response["live_error"] = str(exc)

    return response
