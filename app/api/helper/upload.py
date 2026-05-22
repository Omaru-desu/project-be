from app.services.supabase_service import get_supabase_client
from fastapi import HTTPException
from google.cloud import storage
from datetime import timedelta

storage_client = storage.Client()
supabase = get_supabase_client()

def create_upload_record(
    upload_id: str,
    project_id: str,
    owner: str,
    upload_type: str,
    project_type: str,
    bucket: str,
    raw_gcs_uri: str | None,
    source_filename: str | None,
    status: str,
    frame_count: int = 0,
    name: str | None = None,
):
    payload = {
        "id": upload_id,
        "project_id": project_id,
        "owner": owner,
        "upload_type": upload_type,
        "project_type": project_type,
        "bucket": bucket,
        "raw_gcs_uri": raw_gcs_uri,
        "source_filename": source_filename,
        "status": status,
        "frame_count": frame_count,
        "name": name,   
    }

    result = supabase.table("uploads").insert(payload).execute()
    if not result.data:
        raise HTTPException(status_code=500, detail="Failed to create upload record")

    return result.data[0]


def update_upload_record(upload_id: str, update_data: dict):
    result = (
        supabase
        .table("uploads")
        .update(update_data)
        .eq("id", upload_id)
        .execute()
    )

    if not result.data:
        raise HTTPException(status_code=500, detail="Failed to update upload record")

    return result.data[0]


def insert_frame_records(frames: list[dict]):
    if not frames:
        return []

    result = supabase.table("frames").insert(frames).execute()
    if not result.data:
        raise HTTPException(status_code=500, detail="Failed to insert frame records")

    return result.data

def get_project_for_user(project_id: str, user_id: str):
    result = (
        supabase
        .table("projects")
        .select("*")
        .eq("id", project_id)
        .eq("owner", user_id)
        .execute()
    )

    if not result.data:
        raise HTTPException(status_code=404, detail="Project not found")

    project = result.data[0]

    return project

def update_frame_record(frame_id: str, update_data: dict):
    result = (
        supabase
        .table("frames")
        .update(update_data)
        .eq("id", frame_id)
        .execute()
    )
    return result.data[0] if result.data else None


def get_frames_for_upload(upload_id: str):
    try:
        result = (
            supabase
            .table("frames")
            .select("*")
            .eq("upload_id", upload_id)
            .execute()
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Failed to retrieve frames") from exc

    return result.data or []

def generate_signed_url(gcs_uri: str) -> str:
    # gs://bucket/path/to/file.jpg → bucket + blob
    parts = gcs_uri.replace("gs://", "").split("/", 1)
    bucket_name = parts[0]
    blob_name = parts[1]

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(hours=24),
        method="GET",
    )

    return url

def get_project_frames_with_detections(project_id: str):
    try:
        uploads_res = (
            supabase
            .table("uploads")
            .select("id")
            .eq("project_id", project_id)
            .execute()
        )
        uploads = uploads_res.data or []

        upload_ids = [u["id"] for u in uploads]

        if not upload_ids:
            return []

        frames_res = (
            supabase
            .table("frames")
            .select("*")
            .in_("upload_id", upload_ids)
            .execute()
        )
        frames = frames_res.data or []

        frame_ids = [f["id"] for f in frames]

        detections_res = (
            supabase
            .table("detections")
            .select("*")
            .in_("frame_id", frame_ids)
            .neq("is_deleted", True)
            .execute()
        )
        detections = detections_res.data or []

    except Exception as exc:
        raise HTTPException(status_code=500, detail="Failed to fetch project data") from exc

    detections_map = {}
    for det in detections:
        fid = det["frame_id"]
        if fid not in detections_map:
            detections_map[fid] = []

        detections_map[fid].append({
            "id": det["id"],
            "bbox": det["bbox"],
            "label_id": det["label_id"],
            "status": det["status"],
            "seen": det.get("seen", False),
            "taxon": det.get("taxon"),
            "display_label": det.get("display_label"),
            "score": det.get("score"),
            "track_id": det.get("track_id"),
            "family": det.get("family"),
            "family_confidence": det.get("family_confidence"),
        })

    result = []
    for frame in frames:
        signed_url = generate_signed_url(frame["frame_gcs_uri"])

        result.append({
            "id": frame["id"],
            "upload_id": frame["upload_id"],
            "source_filename": frame["source_filename"],
            "frame_gcs_uri": frame["frame_gcs_uri"],
            "frame_url": signed_url,
            "status": frame["status"],
            "is_approved": frame.get("is_approved", False), 
            "detections": detections_map.get(frame["id"], [])
        })

    return result

def get_detections_by_frame(project_id: str, frame_id: str, user_id: str):
    project = (
        supabase
        .table("projects")
        .select("*")
        .eq("id", project_id)
        .eq("owner", user_id)
        .single()
        .execute()
    )

    if not project.data:
        raise HTTPException(status_code=404, detail="Project not found")

    frame = (
        supabase
        .table("frames")
        .select("*")
        .eq("id", frame_id)
        .single()
        .execute()
    )

    if not frame.data:
        raise HTTPException(status_code=404, detail="Frame not found")

    # verify frame belongs to project
    if frame.data["project_id"] != project_id:
        raise HTTPException(status_code=403, detail="Frame does not belong to this project")

    detections_res = (
        supabase
        .table("detections")
        .select("*")
        .eq("frame_id", frame_id)
        .neq("is_deleted", True)
        .execute()
    )

    detections = detections_res.data or []

    return {
        "frame_id": frame_id,
        "detections": detections
    }

def get_datasets_for_project(project_id: str):
    try:
        uploads_res = (
            supabase
            .table("uploads")
            .select("id, name, status, frame_count, frames_processed, created_at, upload_type")
            .eq("project_id", project_id)
            .order("created_at", desc=True)
            .execute()
        )
        uploads = uploads_res.data or []

        upload_ids = [u["id"] for u in uploads]

        if not upload_ids:
            return {
                "datasets": [],
                "total_frames": 0,
                "reviewed_frames": 0,
                "active_datasets": 0,
            }

        frames_res = (
            supabase
            .table("frames")
            .select("id, upload_id, is_approved")
            .in_("upload_id", upload_ids)
            .execute()
        )
        frames = frames_res.data or []

    except Exception as exc:
        raise HTTPException(status_code=500, detail="Failed to fetch datasets") from exc

    total_frames = sum(u["frame_count"] or 0 for u in uploads)

    reviewed_frames = sum(1 for f in frames if f.get("is_approved") is True)

    active_datasets = sum(1 for u in uploads if u["status"] not in ("ready", "failed"))

    reviewed_per_upload = {}
    for f in frames:
        uid = f["upload_id"]
        if uid not in reviewed_per_upload:
            reviewed_per_upload[uid] = 0
        if f.get("is_approved") is True:
            reviewed_per_upload[uid] += 1

    datasets = []
    for u in uploads:
        uid = u["id"]
        datasets.append({
            "id": uid,
            "name": u["name"],
            "status": u["status"],
            "frame_count": u["frame_count"] or 0,
            "frames_processed": u["frames_processed"] or 0,
            "reviewed_frames": reviewed_per_upload.get(uid, 0),
            "created_at": u["created_at"],
            "upload_type": u["upload_type"],
        })

    return {
        "datasets": datasets,
        "total_frames": total_frames,
        "reviewed_frames": reviewed_frames,
        "active_datasets": active_datasets,
    }

def get_upload_frames_paginated(upload_id: str, page: int, limit: int):
    offset = (page - 1) * limit

    try:
        count_res = (
            supabase
            .table("frames")
            .select("id", count="exact")
            .eq("upload_id", upload_id)
            .execute()
        )
        total = count_res.count or 0

        frames_res = (
            supabase
            .table("frames")
            .select("id, source_filename, is_approved")
            .eq("upload_id", upload_id)
            .order("source_filename")
            .range(offset, offset + limit - 1)
            .execute()
        )
        frames = frames_res.data or []

    except Exception as exc:
        raise HTTPException(status_code=500, detail="Failed to fetch frames") from exc

    return {
        "frames": frames,
        "total": total,
        "page": page,
        "total_pages": max(1, -(-total // limit)),  # ceiling division
    }