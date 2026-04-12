from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.auth import get_current_user
from app.api.helper.upload import get_project_for_user
from app.services.supabase_service import get_supabase_client

router = APIRouter()

supabase = get_supabase_client()


class LabelToggle(BaseModel):
    label_id: str
    enabled: bool


@router.get("/projects/{project_id}/labels")
def get_project_labels(
    project_id: str,
    user_id: str = Depends(get_current_user),
):
    get_project_for_user(project_id, user_id)

    result = (
        supabase
        .table("project_labels")
        .select("label_id, enabled")
        .eq("project_id", project_id)
        .execute()
    )

    return result.data or []


@router.put("/projects/{project_id}/labels")
def set_project_labels(
    project_id: str,
    labels: list[LabelToggle],
    user_id: str = Depends(get_current_user),
):
    get_project_for_user(project_id, user_id)

    rows = [
        {"project_id": project_id, "label_id": l.label_id, "enabled": l.enabled}
        for l in labels
    ]

    try:
        result = (
            supabase
            .table("project_labels")
            .upsert(rows, on_conflict="project_id,label_id")
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to update labels: {exc}") from exc

    return result.data or []


@router.patch("/projects/{project_id}/labels/{label_id}")
def toggle_label(
    project_id: str,
    label_id: str,
    body: LabelToggle,
    user_id: str = Depends(get_current_user),
):
    get_project_for_user(project_id, user_id)

    try:
        result = (
            supabase
            .table("project_labels")
            .upsert(
                {"project_id": project_id, "label_id": label_id, "enabled": body.enabled},
                on_conflict="project_id,label_id",
            )
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to toggle label: {exc}") from exc

    return result.data[0] if result.data else {}