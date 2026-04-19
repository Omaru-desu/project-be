import os
import uuid
import tempfile
from typing import List

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, BackgroundTasks

from PIL import Image
from io import BytesIO

from app.auth import get_current_user
from app.services.gcp_storage import upload_to_gcp, get_bucket_name
from app.services.video_processor import extract_frames
from app.api.helper.upload import create_upload_record, update_upload_record, insert_frame_records, get_project_for_user, get_project_frames_with_detections
from app.api.helper.segment import get_active_label_ids
from app.services.process_service import process_upload

router = APIRouter()

@router.post("/projects/{project_id}/upload")
async def upload_files(
    project_id: str,
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...),
    user_id: str = Depends(get_current_user)
):
    project = get_project_for_user(project_id, user_id)
    project_type = project["type"]
    bucket_name = get_bucket_name(project_type)

    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    upload_id = str(uuid.uuid4())

    image_files = []
    video_files = []

    for file in files:
        if not file.content_type:
            raise HTTPException(status_code=400, detail=f"Missing content type for {file.filename}")

        if file.content_type.startswith("image"):
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

    raise HTTPException(status_code=400, detail="Unsupported upload request")

@router.get("/projects/{project_id}/frames")
def get_project_frames(
    project_id: str,
    user_id: str = Depends(get_current_user),
):
    get_project_for_user(project_id, user_id) # check ownership

    frames = get_project_frames_with_detections(project_id)

    return {"frames": frames}