from app.services.supabase_service import get_supabase_client
from fastapi import HTTPException

supabase = get_supabase_client()


def get_detection_embedding(detection_id: str) -> list[float] | None:
    result = (
        supabase
        .table("detection_embeddings")
        .select("embedding")
        .eq("id", detection_id)
        .execute()
    )
    if not result.data:
        return None
    return result.data[0]["embedding"]


def get_clip_detection_embedding(detection_id: str) -> list[float] | None:
    result = (
        supabase
        .table("clip_detection_embeddings")
        .select("embedding")
        .eq("id", detection_id)
        .execute()
    )
    if not result.data:
        return None
    return result.data[0]["embedding"]


def find_similar_detections(
    query_embedding: list[float],
    project_id: str,
    exclude_detection_id: str,
    limit: int = 10,
) -> list[dict]:
    try:
        result = (
            supabase
            .rpc("match_detection_embeddings", {
                "query_embedding": query_embedding,
                "match_project_id": project_id,
                "match_count": limit,
                "exclude_detection_id": exclude_detection_id,
            })
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Similarity search failed: {exc}") from exc

    return result.data or []


CLIP_SIMILARITY_THRESHOLD = 0.28


def find_detections_by_text(
    query_embedding: list[float],
    project_id: str,
    limit: int = 10,
) -> list[dict]:
    try:
        result = (
            supabase
            .rpc("match_clip_detection_embeddings", {
                "query_embedding": query_embedding,
                "match_project_id": project_id,
                "match_count": limit,
            })
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"CLIP similarity search failed: {exc}") from exc

    return [r for r in (result.data or []) if r.get("similarity", 0) >= CLIP_SIMILARITY_THRESHOLD]
