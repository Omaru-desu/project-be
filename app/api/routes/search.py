from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth import get_current_user
from app.api.helper.upload import get_project_for_user, generate_signed_url
from app.api.helper.search import get_detection_embedding, get_clip_detection_embedding, find_similar_detections, find_detections_by_text
from app.services.model_service import embed_text
from app.services.supabase_service import get_supabase_client

_SMALL_KEYWORDS = {"small", "tiny", "little", "miniature", "minute", "small-sized"}
_LARGE_KEYWORDS = {"large", "big", "huge", "giant", "massive", "enormous", "great", "large-sized"}


def _bbox_area(bbox) -> float:
    if not bbox or len(bbox) < 4:
        return 0.0
    x1, y1, x2, y2 = bbox[0], bbox[1], bbox[2], bbox[3]
    return max(0.0, float(x2 - x1)) * max(0.0, float(y2 - y1))


def _size_bias(query: str) -> int:
    tokens = set(query.lower().split())
    if tokens & _SMALL_KEYWORDS:
        return -1  # prefer smaller
    if tokens & _LARGE_KEYWORDS:
        return 1   # prefer larger
    return 0


def _rerank_by_size(results: list[dict], bias: int) -> list[dict]:
    areas = [_bbox_area(r["bbox"]) for r in results]
    max_area = max(areas) if areas else 1.0
    if max_area == 0:
        return results

    for r, area in zip(results, areas):
        norm = area / max_area  # 0 = smallest, 1 = largest
        size_score = (1.0 - norm) if bias == -1 else norm
        r["_rank"] = (r["similarity"] or 0) * 0.6 + size_score * 0.4

    results.sort(key=lambda x: x.pop("_rank"), reverse=True)
    return results

router = APIRouter()
supabase = get_supabase_client()


@router.get("/projects/{project_id}/detections/{detection_id}/similar")
def get_similar_detections(
    project_id: str,
    detection_id: str,
    limit: int = Query(default=10, ge=1, le=50),
    user_id: str = Depends(get_current_user),
):
    get_project_for_user(project_id, user_id)

    det_res = (
        supabase
        .table("detections")
        .select("id, project_id")
        .eq("id", detection_id)
        .eq("project_id", project_id)
        .single()
        .execute()
    )
    if not det_res.data:
        raise HTTPException(status_code=404, detail="Detection not found")

    embedding = get_detection_embedding(detection_id)
    if embedding is None:
        raise HTTPException(status_code=404, detail="No DINOv3 embedding found for this detection — re-process the upload")

    matches = find_similar_detections(embedding, project_id, detection_id, limit)
    if not matches:
        return {"detection_id": detection_id, "results": []}

    matched_ids = [m["detection_id"] for m in matches]
    similarity_map = {m["detection_id"]: m["similarity"] for m in matches}

    dets_res = (
        supabase
        .table("detections")
        .select("id, frame_id, label_id, display_label, bbox, score, crop_gcs_uri")
        .in_("id", matched_ids)
        .execute()
    )

    results = []
    for det in (dets_res.data or []):
        crop_url = None
        if det.get("crop_gcs_uri"):
            try:
                crop_url = generate_signed_url(det["crop_gcs_uri"])
            except Exception:
                pass

        results.append({
            "detection_id": det["id"],
            "frame_id": det["frame_id"],
            "label_id": det["label_id"],
            "display_label": det["display_label"],
            "bbox": det["bbox"],
            "score": det["score"],
            "crop_url": crop_url,
            "similarity": similarity_map.get(det["id"]),
        })

    results.sort(key=lambda x: x["similarity"] or 0, reverse=True)

    return {"detection_id": detection_id, "results": results}


@router.get("/projects/{project_id}/search")
async def text_search_detections(
    project_id: str,
    q: str = Query(..., min_length=1, description="Natural language search query"),
    limit: int = Query(default=10, ge=1, le=50),
    user_id: str = Depends(get_current_user),
):
    get_project_for_user(project_id, user_id)

    try:
        query_embedding = await embed_text(q)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to embed query text: {exc}") from exc

    matches = find_detections_by_text(query_embedding, project_id, limit)
    if not matches:
        return {"query": q, "results": []}

    matched_ids = [m["detection_id"] for m in matches]
    similarity_map = {str(m["detection_id"]): m["similarity"] for m in matches}

    try:
        dets_res = (
            supabase
            .table("detections")
            .select("id, frame_id, label_id, display_label, bbox, score, crop_gcs_uri")
            .in_("id", matched_ids)
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch detection metadata: {exc}") from exc

    results = []
    for det in (dets_res.data or []):
        crop_url = None
        if det.get("crop_gcs_uri"):
            try:
                crop_url = generate_signed_url(det["crop_gcs_uri"])
            except Exception:
                pass

        results.append({
            "detection_id": det["id"],
            "frame_id": det["frame_id"],
            "label_id": det["label_id"],
            "display_label": det["display_label"],
            "bbox": det["bbox"],
            "score": det["score"],
            "crop_url": crop_url,
            "similarity": similarity_map.get(str(det["id"])),
        })

    bias = _size_bias(q)
    if bias != 0:
        results = _rerank_by_size(results, bias)
    else:
        results.sort(key=lambda x: x["similarity"] or 0, reverse=True)

    return {"query": q, "results": results}
