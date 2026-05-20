import logging
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel

from app.auth import get_current_user
from app.services.supabase_service import get_supabase_client

from app.services import model_service

router = APIRouter()
supabase = get_supabase_client()
logger = logging.getLogger(__name__)


class ReviewDetectionLabel(BaseModel):
    display_label: str


def _derive_label_id(display_label: str) -> str:
    return display_label.strip().lower().replace(" ", "_")


def _get_detection_and_verify_owner(detection_id: str, user_id: str) -> dict:
    det_res = (
        supabase
        .table("detections")
        .select("*")
        .eq("id", detection_id)
        .single()
        .execute()
    )

    if not det_res.data:
        raise HTTPException(status_code=404, detail="Detection not found")

    det = det_res.data
    project_id = det["project_id"]

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

    return det


@router.get("/detections/{detection_id}/review")
def get_detection_for_review(
    detection_id: str,
    user_id: str = Depends(get_current_user),
):
    det = _get_detection_and_verify_owner(detection_id, user_id)

    frame_res = (
        supabase
        .table("frames")
        .select("frame_gcs_uri")
        .eq("id", det["frame_id"])
        .single()
        .execute()
    )
    frame_gcs_uri = frame_res.data["frame_gcs_uri"] if frame_res.data else None

    return {
        "id": det["id"],
        "frame_id": det["frame_id"],
        "project_id": det["project_id"],
        "display_label": det["display_label"],
        "label_id": det["label_id"],
        "status": det["status"],
        "bbox": det["bbox"],
        "score": det.get("score"),
        "blur_score": det.get("blur_score"),
        "crop_gcs_uri": det.get("crop_gcs_uri"),
        "frame_gcs_uri": frame_gcs_uri,
    }


@router.patch("/detections/{detection_id}/label")
async def review_detection_label(
    detection_id: str,
    body: ReviewDetectionLabel,
    user_id: str = Depends(get_current_user),
):
    det = _get_detection_and_verify_owner(detection_id, user_id)
    project_id = det["project_id"]
    derived_label_id = _derive_label_id(body.display_label)

    try:
        update_res = (
            supabase
            .table("detections")
            .update({
                "label_id": derived_label_id,
                "display_label": body.display_label,
                "status": "reviewed",
            })
            .eq("id", detection_id)
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to update detection: {exc}") from exc

    try:
        if project_id:
            supabase.table("projects").update({"updated_at": datetime.now(timezone.utc).isoformat()}).eq("id", project_id).execute()
    except Exception:
        pass

    return update_res.data[0] if update_res.data else {}


@router.delete("/detections/{detection_id}")
def delete_detection(
    detection_id: str,
    user_id: str = Depends(get_current_user),
):
    _get_detection_and_verify_owner(detection_id, user_id)

    try:
        supabase.table("detections").update({"is_deleted": True}).eq("id", detection_id).execute()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to delete detection: {exc}") from exc

    return {"success": True}


def _ensure_class_indices(project_id: str, display_labels: list[str]) -> dict[str, int]:
    distinct = sorted({lbl for lbl in display_labels if lbl})
    if not distinct:
        logger.info("[retrain %s] class diff: no labels to assign", project_id)
        return {}

    for attempt in range(8):
        existing_res = (
            supabase
            .table("project_classes")
            .select("class_index, display_label")
            .eq("project_id", project_id)
            .execute()
        )
        existing = {row["display_label"]: row["class_index"] for row in (existing_res.data or [])}
        missing = [lbl for lbl in distinct if lbl not in existing]
        if not missing:
            logger.info(
                "[retrain %s] class diff: all %d labels already mapped, no inserts",
                project_id, len(distinct),
            )
            return existing

        next_index = (max(existing.values()) + 1) if existing else 0
        new_rows = []
        for lbl in missing:
            new_rows.append({
                "project_id": project_id,
                "class_index": next_index,
                "display_label": lbl,
            })
            next_index += 1

        logger.info(
            "[retrain %s] class diff attempt %d: inserting %d new classes %s "
            "(existing=%d, next_index=%d)",
            project_id, attempt + 1, len(new_rows),
            [(r["class_index"], r["display_label"]) for r in new_rows],
            len(existing), new_rows[0]["class_index"],
        )

        try:
            supabase.table("project_classes").insert(new_rows).execute()
            existing.update({row["display_label"]: row["class_index"] for row in new_rows})
            return existing
        except Exception as exc:
            logger.warning(
                "[retrain %s] class diff insert collision on attempt %d: %s — retrying",
                project_id, attempt + 1, exc,
            )
            continue

    raise HTTPException(
        status_code=503,
        detail="Could not assign project class indices (contention).",
    )


def _trigger_retrain(project_id: str, model_type: str) -> None:
    logger.info("[retrain %s] trigger start (model_type=%s)", project_id, model_type)
    try:
        reviewed_res = (
            supabase
            .table("detections")
            .select("bbox, display_label, label_id, frame_id")
            .eq("project_id", project_id)
            .eq("status", "reviewed")
            .neq("is_deleted", True)
            .execute()
        )
        rows = reviewed_res.data or []
        logger.info("[retrain %s] fetched %d reviewed detections", project_id, len(rows))

        frame_ids = list({d["frame_id"] for d in rows if d.get("frame_id")})
        frame_uri_by_id: dict[str, str] = {}
        if frame_ids:
            frames_res = (
                supabase
                .table("frames")
                .select("id, frame_gcs_uri")
                .in_("id", frame_ids)
                .execute()
            )
            frame_uri_by_id = {
                f["id"]: f["frame_gcs_uri"]
                for f in (frames_res.data or [])
                if f.get("frame_gcs_uri")
            }
        logger.info(
            "[retrain %s] joined %d frames -> %d resolvable GCS uris",
            project_id, len(frame_ids), len(frame_uri_by_id),
        )

        def _canonical(d: dict) -> str | None:
            slug = d.get("label_id")
            if slug:
                return slug
            disp = d.get("display_label")
            return _derive_label_id(disp) if disp else None

        canonical_labels = [c for c in (_canonical(d) for d in rows) if c]
        class_map = _ensure_class_indices(project_id, canonical_labels)
        logger.info(
            "[retrain %s] class map size=%d: %s",
            project_id, len(class_map),
            sorted(class_map.items(), key=lambda kv: kv[1]),
        )

        annotations: list[dict] = []
        skipped_no_label = 0
        skipped_no_uri = 0
        for d in rows:
            slug = _canonical(d)
            frame_gcs_uri = frame_uri_by_id.get(d.get("frame_id"))
            if not slug:
                skipped_no_label += 1
                continue
            if not frame_gcs_uri:
                skipped_no_uri += 1
                continue
            class_index = class_map.get(slug)
            if class_index is None:
                continue
            annotations.append({
                "frame_gcs_uri": frame_gcs_uri,
                "bbox": d["bbox"],
                "display_label": slug,
                "class_index": class_index,
            })
        logger.info(
            "[retrain %s] built %d annotations (skipped: no_label=%d, no_uri=%d)",
            project_id, len(annotations), skipped_no_label, skipped_no_uri,
        )

        classes_payload = [
            {"class_index": idx, "display_label": lbl}
            for lbl, idx in sorted(class_map.items(), key=lambda kv: kv[1])
        ]

        supabase.table("project_models").update({
            "retrain_status": "queued",
            "retrain_error": None,
        }).eq("project_id", project_id).execute()
        logger.info("[retrain %s] DB set retrain_status=queued", project_id)

        logger.info(
            "[retrain %s] POST -> model service /model/retrain "
            "(model_type=%s, classes=%d, annotations=%d)",
            project_id, model_type, len(classes_payload), len(annotations),
        )
        spawn_result = model_service.retrain_project_sync(
            project_id=project_id,
            model_type=model_type,
            classes=classes_payload,
            annotations=annotations,
        )
        job_id = spawn_result.get("job_id")
        logger.info(
            "[retrain %s] model service returned job_id=%s status=%s",
            project_id, job_id, spawn_result.get("status"),
        )

        supabase.table("project_models").update({
            "retrain_job_id": job_id,
            "retrain_status": spawn_result.get("status", "queued"),
        }).eq("project_id", project_id).execute()
        logger.info("[retrain %s] DB set retrain_job_id and status — trigger done", project_id)
    except Exception as exc:
        logger.exception("[retrain %s] trigger failed", project_id)
        try:
            supabase.table("project_models").update({
                "retrain_status": "failed",
                "retrain_error": f"trigger failed: {exc}",
                "last_retrain_at": datetime.now(timezone.utc).isoformat(),
            }).eq("project_id", project_id).execute()
        except Exception:
            logger.exception("[retrain %s] also failed to record failure to DB", project_id)


@router.post("/projects/{project_id}/frames/{frame_id}/approve")
async def approve_frame(
    project_id: str,
    frame_id: str,
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

    # bulk update all unreviewed detections in this frame to reviewed
    supabase.table("detections").update({
        "status": "reviewed"
    }).eq("frame_id", frame_id).eq("status", "needs_review").execute()

    model_res = (
        supabase
        .table("project_models")
        .select("*")
        .eq("project_id", project_id)
        .single()
        .execute()
    )

    did_retrain = False
    if model_res.data:
        model_row = model_res.data
        current_count = model_row["approved_since_last_retrain"] + 1
        did_retrain = current_count >= 10
        logger.info(
            "[approve %s frame=%s] counter %d -> %d (status=%s)",
            project_id, frame_id, model_row["approved_since_last_retrain"],
            current_count, model_row.get("retrain_status"),
        )

        if did_retrain:
            logger.info(
                "[approve %s frame=%s] threshold hit — resetting counter and scheduling retrain",
                project_id, frame_id,
            )
            supabase.table("project_models").update({
                "approved_since_last_retrain": 0
            }).eq("id", model_row["id"]).execute()

            background_tasks.add_task(
                _trigger_retrain,
                project_id,
                model_row["model_type"],
            )
        else:
            supabase.table("project_models").update({
                "approved_since_last_retrain": current_count
            }).eq("id", model_row["id"]).execute()

    supabase.table("frames").update({
        "is_approved": True
    }).eq("id", frame_id).execute()

    return {"retrained": did_retrain}
