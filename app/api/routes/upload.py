import tempfile
from fastapi import APIRouter, UploadFile, File
import uuid
from app.services.gcp_storage import upload_to_gcp
import os
from app.services.video_processor import extract_frames

router = APIRouter()

@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    content = await file.read()
    if file.content_type.startswith("image"):

        unique_name = f"{uuid.uuid4()}_{file.filename}"

        url = upload_to_gcp(content, unique_name, file.content_type)

        return {"url": url}
    
    elif file.content_type.startswith("video"):
        with tempfile.TemporaryDirectory() as temp_dir:
            video_path = os.path.join(temp_dir, "video.mp4")

            with open(video_path, "wb") as f:
                f.write(content)

            frame_paths = extract_frames(video_path, temp_dir)

            urls = []
            for frame_path in frame_paths:
                with open(frame_path, "rb") as f:
                    unique_name = f"{uuid.uuid4()}_{os.path.basename(frame_path)}"
                    urls.append(upload_to_gcp(f.read(), unique_name, "image/jpeg"))

            return {"frames": urls}
            
    return {"error": "Unsupported file type"}