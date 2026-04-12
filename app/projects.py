import os
from typing import List
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from dotenv import load_dotenv
from supabase import create_client, Client
from typing import Optional

from app.auth import get_current_user

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

router = APIRouter()

class ProjectCreate(BaseModel):
    name: str
    description: str
    type: str
    frame_count: int


class ProjectResponse(BaseModel):
    id: str
    name: str
    description: str
    type: str
    frame_count: int
    created_at: datetime
    owner: str

class ProjectUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    frame_count: Optional[int] = None

@router.post("/projects", response_model=ProjectResponse, status_code=status.HTTP_201_CREATED)
def create_project(
    project: ProjectCreate,
    user_id: str = Depends(get_current_user)
):
    payload = {
        "name": project.name,
        "description": project.description,
        "type": project.type,
        "frame_count": project.frame_count,
        "owner": user_id
    }

    try:
        result = supabase.table("projects").insert(payload).execute()

        if not result.data:
            raise HTTPException(status_code=500, detail="Failed to create project")

        return result.data[0]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/projects", response_model=List[ProjectResponse])
def get_projects(user_id: str = Depends(get_current_user)):
    try:
        result = (
            supabase
            .table("projects")
            .select("*")
            .eq("owner", user_id)
            .execute()
        )

        return result.data or []

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/projects/{project_id}", response_model=ProjectResponse)
def get_project(project_id: str, user_id: str = Depends(get_current_user)):
    try:
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

        return result.data[0]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/projects/{project_id}", response_model=ProjectResponse)
def update_project(
    project_id: str,
    project: ProjectUpdate,
    user_id: str = Depends(get_current_user)
):
    update_data = project.model_dump(exclude_unset=True)

    if not update_data:
        raise HTTPException(status_code=400, detail="No data to update")

    result = (
        supabase
        .table("projects")
        .update(update_data)
        .eq("id", project_id)
        .eq("owner", user_id)
        .execute()
    )

    if not result.data:
        raise HTTPException(status_code=404, detail="Project not found")

    return result.data[0]

@router.delete("/projects/{project_id}")
def delete_project(
    project_id: str,
    user_id: str = Depends(get_current_user)
):
    result = (
        supabase
        .table("projects")
        .delete()
        .eq("id", project_id)
        .eq("owner", user_id)
        .execute()
    )

    if not result.data:
        raise HTTPException(status_code=404, detail="Project not found")

    return {"message": "Deleted"}