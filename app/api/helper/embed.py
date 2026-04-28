from app.services.supabase_service import get_supabase_client
from fastapi import HTTPException
supabase = get_supabase_client()

def get_detections_for_upload(upload_id: str) -> list[dict]:
    try:
        result = (
            supabase
            .table("detections")
            .select("*")
            .eq("upload_id", upload_id)
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Failed to retrieve detections") from exc

    return result.data or []


def upsert_detection_embeddings(rows: list[dict]) -> list[dict]:
    if not rows:
        return []

    try:
        result = (
            supabase
            .table("detection_embeddings")
            .upsert(rows, on_conflict="id")
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to upsert detection embeddings: {exc}") from exc

    return result.data or []


def upsert_frame_embeddings(rows: list[dict]) -> list[dict]:
    if not rows:
        return []

    try:
        result = (
            supabase
            .table("frame_embeddings")
            .upsert(rows, on_conflict="id")
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to upsert frame embeddings: {exc}") from exc

    return result.data or []