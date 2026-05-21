import logging

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException

from app.auth import get_current_user
from app.services import model_service
from app.services.supabase_service import get_supabase_client

router = APIRouter()
supabase = get_supabase_client()
logger = logging.getLogger(__name__)


@router.get("/projects/{project_id}/retrain/status")
async def get_retrain_status(
    project_id: str,
    background_tasks: BackgroundTasks,
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
            "checkpoint_url, model_type, approved_since_last_retrain, "
            "retrain_pending, retrain_consecutive_failures"
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
        "retrain_pending": row.get("retrain_pending"),
        "retrain_consecutive_failures": row.get("retrain_consecutive_failures"),
    }

    if row.get("retrain_job_id") and row.get("retrain_status") in ("queued", "training"):
        try:
            live = await model_service.get_retrain_job(row["retrain_job_id"])
            response["live"] = live
        except Exception as exc:
            response["live_error"] = str(exc)

    if (
        row.get("retrain_status") in ("ready", "failed")
        and row.get("retrain_pending") is True
    ):
        from app.api.routes.review import _drain_pending_slot, _trigger_retrain
        try:
            drained = _drain_pending_slot(project_id)
        except HTTPException:
            logger.exception("[drain-backstop %s] drain RPC missing", project_id)
            drained = False
        if drained:
            model_type = row.get("model_type") or "pretrained"
            logger.info(
                "[drain-backstop %s] claimed via status poll -> "
                "scheduling follow-up retrain (model_type=%s)",
                project_id, model_type,
            )
            background_tasks.add_task(_trigger_retrain, project_id, model_type)
            response["drain_scheduled"] = True

    return response
