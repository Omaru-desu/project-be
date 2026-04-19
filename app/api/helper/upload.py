from app.services.supabase_service import get_supabase_client
from fastapi import HTTPException

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