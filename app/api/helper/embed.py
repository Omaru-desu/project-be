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


def upsert_detection_embeddings(rows: list[dict], chunk_size: int = 50) -> list[dict]:
    if not rows:
        return []

    all_data = []
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        try:
            result = (
                supabase
                .table("detection_embeddings")
                .upsert(chunk, on_conflict="id")
                .execute()
            )
            all_data.extend(result.data or [])
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to upsert detection embeddings: {exc}") from exc

    return all_data


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
