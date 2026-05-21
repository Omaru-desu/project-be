from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel
from collections import defaultdict

from app.auth import get_current_user
from app.services.supabase_service import get_supabase_client

router = APIRouter()
supabase = get_supabase_client()

class ClassMetric(BaseModel):
    label: str
    precision: float  
    recall: float
    f1: float
    samples: int       
class ModelPerformanceResponse(BaseModel):
    map_at_05: float   
    precision: float      
    recall: float          
    uncertain_count: int 
    per_class: list[ClassMetric]

def _safe_div(a: int, b: int) -> float:
    return a / b if b else 0.0

def _f1(p: float, r: float) -> float:
    return 2 * p * r / (p + r) if (p + r) else 0.0

@router.get("/model-performance", response_model=ModelPerformanceResponse)
def get_model_performance(
    project_id: str = Query(..., description="Project UUID to evaluate"),
    current_user: str = Depends(get_current_user),
):
    try:
        response = (
            supabase.table("detections")
            .select("annotation_source, status, original_label, display_label, is_deleted")
            .eq("project_id", project_id)
            .neq("status", "needs_review")
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch detections: {exc}")

    detections = response.data or []

    try:
        uncertain_response = (
            supabase.table("detections")
            .select("id", count="exact")
            .eq("project_id", project_id)
            .eq("status", "needs_review")
            .eq("is_deleted", False)
            .execute()
        )
        uncertain_count = uncertain_response.count or 0
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch uncertain count: {exc}")

    stats: dict[str, dict[str, int]] = defaultdict(lambda: {"tp": 0, "fp": 0, "fn": 0})

    for d in detections:
        source         = d.get("annotation_source")
        status         = d.get("status")
        original_label = d.get("original_label") or d.get("display_label")
        display_label  = d.get("display_label")

        if source == "machine":
            if d.get("is_deleted"):
                stats[original_label]["fp"] += 1

            elif status == "reviewed":
                if original_label == display_label:
                    stats[original_label]["tp"] += 1
                else:
                    stats[original_label]["fp"] += 1
                    stats[display_label]["fn"]  += 1

        elif source == "human" and status == "reviewed":
            stats[display_label]["fn"] += 1

    per_class: list[ClassMetric] = []
    all_precisions: list[float] = []
    all_recalls: list[float] = []
    all_f1s: list[float] = []

    for label, s in sorted(stats.items(), key=lambda x: x[1]["tp"], reverse=True):
        tp, fp, fn = s["tp"], s["fp"], s["fn"]
        samples = tp + fn  

        precision = _safe_div(tp, tp + fp)
        recall    = _safe_div(tp, tp + fn)
        f1        = _f1(precision, recall)

        per_class.append(ClassMetric(
            label=label,
            precision=round(precision * 100, 1),
            recall=round(recall * 100, 1),
            f1=round(f1 * 100, 1),
            samples=samples,
        ))

        if samples > 0:
            all_precisions.append(precision)
            all_recalls.append(recall)
            all_f1s.append(f1)

    n = len(all_precisions) or 1
    macro_precision = sum(all_precisions) / n
    macro_recall    = sum(all_recalls) / n
    map_at_05       = sum(all_f1s) / n    

    return ModelPerformanceResponse(
        map_at_05=round(map_at_05 * 100, 1),
        precision=round(macro_precision * 100, 1),
        recall=round(macro_recall * 100, 1),
        uncertain_count=uncertain_count,
        per_class=per_class,
    )