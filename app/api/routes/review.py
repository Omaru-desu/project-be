import logging
import threading
import time
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel

from app.auth import get_current_user
from app.services.supabase_service import get_supabase_client

from app.services import model_service

router = APIRouter()
supabase = get_supabase_client()
logger = logging.getLogger(__name__)

RETRAIN_THRESHOLD = 10
MAX_CONSECUTIVE_FAILURES = 3
DRAIN_POLL_INTERVAL_S = 30
DRAIN_MAX_WAIT_S = 6 * 60 * 60


def _rpc_fail_loudly(rpc_name: str, exc: Exception) -> None:
    msg = str(exc)
    if rpc_name in msg or "function" in msg.lower() or "PGRST202" in msg:
        raise HTTPException(
            status_code=500,
            detail=(
                f"Postgres function `{rpc_name}` is missing. The atomic "
                "retrain trigger requires this function plus the columns "
                "`retrain_pending bool` and `retrain_consecutive_failures int` "
                "on project_models. See the docstring at the top of "
                "app/api/routes/review.py for the exact DDL and apply it "
                "out of band before using this build."
            ),
        ) from exc
    raise HTTPException(status_code=500, detail=f"{rpc_name} failed: {exc}") from exc


def _claim_retrain_slot(project_id: str) -> str:
    try:
        res = supabase.rpc(
            "claim_retrain_slot",
            {"p_project_id": project_id, "p_threshold": RETRAIN_THRESHOLD},
        ).execute()
    except Exception as exc:
        _rpc_fail_loudly("claim_retrain_slot", exc)
        return ""  # unreachable, _rpc_fail_loudly always raises

    outcome = res.data if isinstance(res.data, str) else (res.data[0] if res.data else None)
    if outcome not in ("below_threshold", "acquired", "coalesced"):
        raise HTTPException(
            status_code=500,
            detail=f"claim_retrain_slot returned unexpected value: {outcome!r}",
        )
    return outcome


def _drain_pending_slot(project_id: str) -> bool:
    try:
        res = supabase.rpc(
            "drain_retrain_slot",
            {
                "p_project_id": project_id,
                "p_max_consec_failures": MAX_CONSECUTIVE_FAILURES,
            },
        ).execute()
    except Exception as exc:
        _rpc_fail_loudly("drain_retrain_slot", exc)
        return False  # unreachable

    value = res.data if isinstance(res.data, str) else (res.data[0] if res.data else None)
    return value == "drained"


class ReviewDetectionLabel(BaseModel):
    display_label: str


class DetectionPatch(BaseModel):
    seen: bool | None = None
    display_label: str | None = None
    bbox: list[float] | None = None


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
        "family": det.get("family"),
        "family_confidence": det.get("family_confidence"),
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


@router.patch("/detections/{detection_id}")
def patch_detection(
    detection_id: str,
    body: DetectionPatch,
    user_id: str = Depends(get_current_user),
):
    _get_detection_and_verify_owner(detection_id, user_id)

    update: dict = {}
    if body.seen is not None:
        update["seen"] = body.seen
    if body.display_label is not None:
        update["display_label"] = body.display_label
        update["label_id"] = _derive_label_id(body.display_label)
    if body.bbox is not None:
        update["bbox"] = body.bbox

    if not update:
        raise HTTPException(status_code=400, detail="No fields to update")

    try:
        res = (
            supabase
            .table("detections")
            .update(update)
            .eq("id", detection_id)
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to update detection: {exc}") from exc

    return res.data[0] if res.data else {}


@router.delete("/detections/{detection_id}")
def delete_detection(
    detection_id: str,
    user_id: str = Depends(get_current_user),
):
    _get_detection_and_verify_owner(detection_id, user_id)

    try:
        supabase.table("detections").update({
            "is_deleted": True,
        }).eq("id", detection_id).execute()
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
    spawn_succeeded = False
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
        }).eq("project_id", project_id).execute()
        logger.info("[retrain %s] DB set retrain_job_id — trigger done", project_id)
        spawn_succeeded = True
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

    _schedule_drain_watch(project_id)


def _schedule_drain_watch(project_id: str) -> None:
    threading.Thread(
        target=_wait_and_drain_loop,
        args=(project_id,),
        name=f"drain-watch-{project_id}",
        daemon=True,
    ).start()


def _wait_and_drain_loop(project_id: str) -> None:
    logger.info("[drain-watch %s] started", project_id)
    deadline = time.time() + DRAIN_MAX_WAIT_S
    terminal_status: str | None = None
    model_type = "pretrained"
    while time.time() < deadline:
        try:
            res = (
                supabase
                .table("project_models")
                .select("retrain_status, model_type")
                .eq("project_id", project_id)
                .single()
                .execute()
            )
            row = res.data or {}
            status = row.get("retrain_status")
            if status in ("ready", "failed"):
                terminal_status = status
                model_type = row.get("model_type") or "pretrained"
                break
        except Exception:
            logger.exception(
                "[drain-watch %s] poll failed, will retry in %ds",
                project_id, DRAIN_POLL_INTERVAL_S,
            )
        time.sleep(DRAIN_POLL_INTERVAL_S)

    if terminal_status is None:
        logger.warning(
            "[drain-watch %s] timed out after %ds — backstop must drain",
            project_id, DRAIN_MAX_WAIT_S,
        )
        return

    logger.info(
        "[drain-watch %s] observed terminal status=%s, attempting drain",
        project_id, terminal_status,
    )
    try:
        drained = _drain_pending_slot(project_id)
    except HTTPException:
        logger.exception("[drain-watch %s] drain RPC missing", project_id)
        return

    if not drained:
        logger.info(
            "[drain-watch %s] no pending follow-up (or failure-cap reached)",
            project_id,
        )
        return

    logger.info(
        "[drain-watch %s] drained -> re-entering _trigger_retrain "
        "(model_type=%s)",
        project_id, model_type,
    )
    _trigger_retrain(project_id, model_type)


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

    # Mark every non-deleted detection in this frame as seen + reviewed.
    # The legacy `status` field is the only persisted reviewed-marker for now;
    # a future migration adds `reviewed` + `approved_at` for an audit trail.
    supabase.table("detections").update({
        "seen": True,
        "status": "reviewed",
    }).eq("frame_id", frame_id).neq("is_deleted", True).execute()

    outcome = _claim_retrain_slot(project_id)
    did_retrain = outcome == "acquired"

    if did_retrain:
        mt_res = (
            supabase
            .table("project_models")
            .select("model_type")
            .eq("project_id", project_id)
            .single()
            .execute()
        )
        model_type = (mt_res.data or {}).get("model_type") or "pretrained"
        logger.info(
            "[approve %s frame=%s] outcome=acquired model_type=%s -> "
            "scheduling _trigger_retrain via BackgroundTasks",
            project_id, frame_id, model_type,
        )
        background_tasks.add_task(_trigger_retrain, project_id, model_type)
    elif outcome == "coalesced":
        logger.info(
            "[approve %s frame=%s] outcome=coalesced — pending flag set, "
            "drain will pick this up after the current retrain finishes",
            project_id, frame_id,
        )
    else:  # below_threshold
        logger.info(
            "[approve %s frame=%s] outcome=below_threshold",
            project_id, frame_id,
        )

    supabase.table("frames").update({
        "is_approved": True,
    }).eq("id", frame_id).execute()

    return {"retrained": did_retrain, "outcome": outcome}


class RevertRow(BaseModel):
    id: str
    seen: bool
    status: str  # legacy field; the new `reviewed` is restored from `status == "reviewed"`


class RevertApprovalBody(BaseModel):
    detections: list[RevertRow]


@router.post("/projects/{project_id}/frames/{frame_id}/revert-approval")
def revert_frame_approval(
    project_id: str,
    frame_id: str,
    body: RevertApprovalBody,
    user_id: str = Depends(get_current_user),
):
    """
    Roll back a recent approve. The client passes the per-row state that
    existed before the approve, and we restore each row + clear the
    image-level approval flag.

    v1 limitation: not atomic. Per-row updates may partially fail.
    """
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

    for row in body.detections:
        try:
            supabase.table("detections").update({
                "seen": row.seen,
                "status": row.status,
            }).eq("id", row.id).eq("frame_id", frame_id).execute()
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to revert detection {row.id}: {exc}",
            ) from exc

    try:
        supabase.table("frames").update({
            "is_approved": False,
        }).eq("id", frame_id).execute()
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to revert frame approval: {exc}",
        ) from exc

    return {"reverted": len(body.detections)}
