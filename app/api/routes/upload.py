from fastapi import APIRouter, UploadFile, File
import uuid
from app.services.gcp_storage import upload_to_gcp

router = APIRouter()

@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    content = await file.read()

    unique_name = f"{uuid.uuid4()}_{file.filename}"

    url = upload_to_gcp(content, unique_name, file.content_type)

    return {"url": url}