from app.services.supabase_service import get_supabase_client
from fastapi import HTTPException
supabase = get_supabase_client()

def get_active_label_ids(project_id: str) -> list[str] | None:
    try:
        result = (
            supabase
            .table("project_labels")
            .select("label_id")
            .eq("project_id", project_id)
            .eq("enabled", True)
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch project labels: {exc}") from exc

    if not result.data:
        return None

    return [row["label_id"] for row in result.data]


def insert_detection_records(detections: list[dict]) -> list[dict]:
    if not detections:
        return []

    try:
        result = (
            supabase
            .table("detections")
            .upsert(detections, on_conflict="id")
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to insert detections: {exc}") from exc

    return result.data or []