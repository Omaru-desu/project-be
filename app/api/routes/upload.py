import os
import uuid
import tempfile
from typing import List

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, BackgroundTasks, Form

from PIL import Image
from io import BytesIO


from app.auth import get_current_user
from app.services.gcp_storage import upload_to_gcp, get_bucket_name
from app.services.video_processor import extract_frames
from app.api.helper.upload import create_upload_record, get_upload_frames_paginated, update_upload_record, insert_frame_records, get_project_for_user, get_project_frames_with_detections, get_detections_by_frame, get_datasets_for_project, generate_signed_url
from app.services.supabase_service import get_supabase_client
from app.api.helper.segment import get_active_label_ids
from app.services.process_service import process_upload
import asyncio
from app.services.model_service import warmup
from app.services.gcp_storage import upload_file_to_gcp


router = APIRouter()
supabase = get_supabase_client()

@router.post("/projects/{project_id}/upload")
async def upload_files(
    project_id: str,
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...),
    name: str = Form(...), 
    user_id: str = Depends(get_current_user)
):
    asyncio.create_task(warmup())
    project = get_project_for_user(project_id, user_id)
    project_type = project["type"]
    bucket_name = get_bucket_name(project_type)

    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    upload_id = str(uuid.uuid4())

    image_files = []
    video_files = []
    rosbag_files = []

    for file in files:
        if not file.content_type:
            raise HTTPException(status_code=400, detail=f"Missing content type for {file.filename}")

        if file.filename and (file.filename.endswith(".bag") or file.filename.endswith(".db3")):
            rosbag_files.append(file)
        elif file.content_type.startswith("image"):
            image_files.append(file)
        elif file.content_type.startswith("video"):
            video_files.append(file)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {file.filename}")

    if image_files and video_files:
        raise HTTPException(
            status_code=400,
            detail="Please upload either images or a video in one request, not both"
        )
    
    if rosbag_files and (image_files or video_files):
        raise HTTPException(
            status_code=400,
            detail="Please upload only one file type at a time"
        )

    if len(video_files) > 1:
        raise HTTPException(
            status_code=400,
            detail="Only one video file is allowed per upload"
        )

    if image_files:
        upload_type = "image_batch" if len(image_files) > 1 else "image"
        raw_prefix = f"gs://{bucket_name}/projects/{project_id}/uploads/{upload_id}/raw/"

        create_upload_record(
            upload_id=upload_id,
            project_id=project_id,
            owner=user_id,
            upload_type=upload_type,
            project_type=project_type,
            bucket=bucket_name,
            raw_gcs_uri=raw_prefix,
            source_filename=None if len(image_files) > 1 else image_files[0].filename,
            status="processing",
            frame_count=0,
            name=name, 
        )

        uploaded_frames = []
        frame_rows = []
        frame_bytes_map = {}

        for idx, file in enumerate(image_files):
            content = await file.read()
            if not content:
                raise HTTPException(status_code=400, detail=f"Empty file: {file.filename}")

            raw_path = f"projects/{project_id}/uploads/{upload_id}/raw/{file.filename}"
            upload_to_gcp(
                file_bytes=content,
                bucket_name=bucket_name,
                destination_blob_name=raw_path,
                content_type=file.content_type,
            )

            frame_id = f"{upload_id}_{idx:06d}"
            frame_filename = f"frame_{idx:06d}.jpg"
            frame_gcs_path = f"projects/{project_id}/uploads/{upload_id}/frames/{frame_filename}"

            image = Image.open(BytesIO(content)).convert("RGB")
            buffer = BytesIO()
            image.save(buffer, format="JPEG", quality=95)
            frame_bytes_map[frame_id] = buffer.getvalue()

            frame_payload = {
                "id": frame_id,
                "project_id": project_id,
                "upload_id": upload_id,
                "owner": user_id,
                "source_filename": file.filename,
                "frame_gcs_uri": f"gs://{bucket_name}/{frame_gcs_path}",
                "status": "queued",
            }

            frame_rows.append(frame_payload)
            uploaded_frames.append(frame_payload)

        insert_frame_records(frame_rows)

        update_upload_record(
            upload_id,
            {
                "frame_count": len(frame_rows),
                "status": "processing_frames",
            }
        )

        label_ids = get_active_label_ids(project_id)
        background_tasks.add_task(
            process_upload,
            upload_id=upload_id,
            project_id=project_id,
            user_id=user_id,
            frame_records=frame_rows,
            label_ids=label_ids,
            frame_bytes_map=frame_bytes_map,
            upload_type=upload_type,
        )

        return {
            "project_id": project_id,
            "project_type": project_type,
            "upload_id": upload_id,
            "type": upload_type,
            "bucket": bucket_name,
            "frame_count": len(uploaded_frames),
            "frames": uploaded_frames,
            "status": "processing_frames",
        }

    if video_files:
        file = video_files[0]
        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="Empty video file")

        raw_path = f"projects/{project_id}/uploads/{upload_id}/raw/{file.filename}"

        raw_uploaded = upload_to_gcp(
            file_bytes=content,
            bucket_name=bucket_name,
            destination_blob_name=raw_path,
            content_type=file.content_type,
        )

        create_upload_record(
            upload_id=upload_id,
            project_id=project_id,
            owner=user_id,
            upload_type="video",
            project_type=project_type,
            bucket=bucket_name,
            raw_gcs_uri=raw_uploaded["gcs_uri"],
            source_filename=file.filename,
            status="processing",
            frame_count=0,
            name=name,
        )

        uploaded_frames = []
        frame_rows = []
        frame_bytes_map = {}

        with tempfile.TemporaryDirectory() as temp_dir:
            video_path = os.path.join(temp_dir, file.filename)

            with open(video_path, "wb") as f:
                f.write(content)

            frames = extract_frames(video_path, temp_dir)

            for idx, frame in enumerate(frames):
                with open(frame["local_path"], "rb") as f:
                    frame_bytes = f.read()

                frame_id = f"{upload_id}_{idx:06d}"
                frame_bytes_map[frame_id] = frame_bytes

                frame_filename = f"frame_{idx:06d}.jpg"
                frame_gcs_path = (
                    f"projects/{project_id}/uploads/{upload_id}/frames/{frame_filename}"
                )

                frame_payload = {
                    "id": frame_id,
                    "source_filename": frame["frame_filename"],
                    "frame_gcs_uri": f"gs://{bucket_name}/{frame_gcs_path}",
                    "status": "queued",
                    "owner": user_id,
                    "upload_id": upload_id,
                    "project_id": project_id,
                }

                frame_rows.append(frame_payload)
                uploaded_frames.append(frame_payload)

        insert_frame_records(frame_rows)

        update_upload_record(
            upload_id,
            {
                "frame_count": len(frame_rows),
                "status": "processing_frames",
            }
        )

        label_ids = get_active_label_ids(project_id)
        background_tasks.add_task(
            process_upload,
            upload_id=upload_id,
            project_id=project_id,
            user_id=user_id,
            frame_records=frame_rows,
            label_ids=label_ids,
            frame_bytes_map=frame_bytes_map,
            upload_type="video",
        )

        return {
            "project_id": project_id,
            "project_type": project_type,
            "upload_id": upload_id,
            "type": "video",
            "bucket": bucket_name,
            "raw_gcs_uri": raw_uploaded["gcs_uri"],
            "frame_count": len(uploaded_frames),
            "frames": uploaded_frames,
            "status": "processing_frames",
        }
 
    if rosbag_files:
        file = rosbag_files[0]
 
        with tempfile.TemporaryDirectory() as temp_dir:
            bag_path = os.path.join(temp_dir, file.filename)
            with open(bag_path, "wb") as f:
                while chunk := await file.read(1024 * 1024): 
                    f.write(chunk)

            raw_path = f"projects/{project_id}/uploads/{upload_id}/raw/{file.filename}"
            raw_uploaded = upload_file_to_gcp(
                file_path=bag_path,
                bucket_name=bucket_name,
                destination_blob_name=raw_path,
                content_type="application/octet-stream",
            )
 
            create_upload_record(
                upload_id=upload_id,
                project_id=project_id,
                owner=user_id,
                upload_type="rosbag",
                project_type=project_type,
                bucket=bucket_name,
                raw_gcs_uri=raw_uploaded["gcs_uri"],
                source_filename=file.filename,
                status="processing",
                frame_count=0,
                name=name,
            )
 
            label_ids = get_active_label_ids(project_id)
            background_tasks.add_task(
                process_upload,
                upload_id=upload_id,
                project_id=project_id,
                user_id=user_id,
                frame_records=[],         
                label_ids=label_ids,
                frame_bytes_map={},       
                upload_type="rosbag",
                rosbag_gcs_uri=raw_uploaded["gcs_uri"],      
                bucket_name=bucket_name,    
            )
 
        return {
            "project_id": project_id,
            "project_type": project_type,
            "upload_id": upload_id,
            "type": "rosbag",
            "bucket": bucket_name,
            "raw_gcs_uri": raw_uploaded["gcs_uri"],
            "frame_count": 0,
            "frames": [],
            "status": "processing",
        }

    raise HTTPException(status_code=400, detail="Unsupported upload request")

@router.get("/projects/{project_id}/frames")
def get_project_frames(
    project_id: str,
    user_id: str = Depends(get_current_user),
):
    get_project_for_user(project_id, user_id) # check ownership

    frames = get_project_frames_with_detections(project_id)

    return {"frames": frames}

@router.get("/projects/{project_id}/frames/{frame_id}/detections")
def get_frame_detections(
    project_id: str,
    frame_id: str,
    user_id: str = Depends(get_current_user),
):
    return get_detections_by_frame(project_id, frame_id, user_id)

@router.get("/projects/{project_id}/datasets")
def get_datasets(
    project_id: str,
    user_id: str = Depends(get_current_user),
):
    get_project_for_user(project_id, user_id)
    return get_datasets_for_project(project_id)

@router.get("/projects/{project_id}/datasets/{upload_id}/frames")
def get_upload_frames(
    project_id: str,
    upload_id: str,
    page: int = 1,
    limit: int = 50,
    user_id: str = Depends(get_current_user),
):
    get_project_for_user(project_id, user_id)
    return get_upload_frames_paginated(upload_id, page, limit)


@router.get("/projects/{project_id}/tracks")
def get_project_tracks(
    project_id: str,
    upload_id: str | None = None,
    user_id: str = Depends(get_current_user),
):
    get_project_for_user(project_id, user_id)

    query = supabase.table("tracks").select("*").eq("project_id", project_id)
    if upload_id:
        query = query.eq("upload_id", upload_id)
    res = query.order("frame_count", desc=True).execute()
    tracks = res.data or []

    for track in tracks:
        rep_uri = track.get("representative_crop_gcs_uri")
        signed: str | None = None
        if rep_uri:
            try:
                signed = generate_signed_url(rep_uri)
            except Exception:
                signed = None
        track["representative_crop_url"] = signed

    return {"tracks": tracks}