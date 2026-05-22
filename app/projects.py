import os
from typing import List
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from dotenv import load_dotenv
from supabase import create_client, Client
from typing import Optional

from app.auth import get_current_user
from app.core.base_classes import BASE_DEIMV2_CLASSES

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
    model_type: str = "pretrained"


class ProjectResponse(BaseModel):
    id: str
    name: str
    description: str
    type: str
    model_type: str
    has_checkpoint: bool
    frame_count: int
    created_at: datetime
    owner: str
    updated_at: Optional[datetime] = None
    reviewed_count: Optional[int] = 0
    detection_count: Optional[int] = 0

class ProjectUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None

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

        project_id = result.data[0]["id"]

        if project.model_type == "pretrained":
            class_rows = [
                {
                    "project_id": project_id,
                    "class_index": class_index,
                    "display_label": display_label,
                }
                for class_index, display_label in BASE_DEIMV2_CLASSES
            ]
            supabase.table("project_classes").insert(class_rows).execute()

        pretrained_url = os.getenv("PRETRAINED_CHECKPOINT_URL")

        model_payload = {
            "project_id": project_id,
            "model_type": project.model_type,
            "checkpoint_url": pretrained_url if project.model_type == "pretrained" else None,
            "approved_since_last_retrain": 0,
        }
        supabase.table("project_models").insert(model_payload).execute()

        project_data = result.data[0]
        project_data["model_type"] = project.model_type
        project_data["has_checkpoint"] = project.model_type == "pretrained"
        return project_data

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/projects", response_model=List[ProjectResponse])
def get_projects(user_id: str = Depends(get_current_user)):
    try:
        projects_res = (
            supabase
            .table("projects")
            .select("*")
            .eq("owner", user_id)
            .execute()
        )
        projects = projects_res.data or []
        if not projects:
            return projects

        stats_res = supabase.rpc("get_project_stats", {"p_owner": str(user_id)}).execute()
        stats_by_id = {
            row["project_id"]: row
            for row in (stats_res.data or [])
        }

        for project in projects:
            count_res = (
                supabase
                .table("frames")
                .select("id", count="exact")
                .eq("project_id", project["id"])
                .execute()
            )
            project["frame_count"] = count_res.count or 0
            model_res = (
                supabase
                .table("project_models")
                .select("model_type, checkpoint_url")
                .eq("project_id", project["id"])
                .limit(1)
                .execute()
            )
            model_data = model_res.data[0] if model_res.data else None

            project["model_type"] = model_data["model_type"] if model_data else "pretrained"
            project["has_checkpoint"] = (
                model_data["model_type"] == "pretrained" or 
                bool(model_data.get("checkpoint_url"))
            ) if model_data else True
        
            s = stats_by_id.get(project["id"], {})
            project["frame_count"]     = s.get("frame_count", 0) or 0
            project["detection_count"] = s.get("detection_count", 0) or 0
            project["reviewed_count"]  = s.get("reviewed_count", 0) or 0

        return projects

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

        project = result.data[0]
        count_res = (
            supabase
            .table("frames")
            .select("id", count="exact")
            .eq("project_id", project_id)
            .execute()
        )
        project["frame_count"] = count_res.count or 0
        model_res = (
            supabase
            .table("project_models")
            .select("model_type, checkpoint_url")
            .eq("project_id", project["id"])
            .limit(1)
            .execute()
        )
        model_data = model_res.data[0] if model_res.data else None

        project["model_type"] = model_data["model_type"] if model_data else "pretrained"
        project["has_checkpoint"] = (
            model_data["model_type"] == "pretrained" or 
            bool(model_data.get("checkpoint_url"))
        ) if model_data else True
        
        return project

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

    updated = result.data[0]

    count_res = (
        supabase
        .table("frames")
        .select("id", count="exact")
        .eq("project_id", project_id)
        .execute()
    )
    updated["frame_count"] = count_res.count or 0

    model_res = (
        supabase
        .table("project_models")
        .select("model_type, checkpoint_url")
        .eq("project_id", project_id)
        .limit(1)
        .execute()
    )
    model_data = model_res.data[0] if model_res.data else None

    updated["model_type"] = model_data["model_type"] if model_data else "pretrained"
    updated["has_checkpoint"] = (
        model_data["model_type"] == "pretrained" or
        bool(model_data.get("checkpoint_url"))
    ) if model_data else True

    return updated

@router.delete("/projects/{project_id}")
def delete_project(
    project_id: str,
    user_id: str = Depends(get_current_user)
):
    project = (
        supabase
        .table("projects")
        .select("id")
        .eq("id", project_id)
        .eq("owner", user_id)
        .execute()
    )

    if not project.data:
        raise HTTPException(status_code=404, detail="Project not found")

    try:
        supabase.table("detection_embeddings").delete().eq("project_id", project_id).execute()
        supabase.table("frame_embeddings").delete().eq("project_id", project_id).execute()
        supabase.table("detections").delete().eq("project_id", project_id).execute()
        supabase.table("project_labels").delete().eq("project_id", project_id).execute()
        supabase.table("project_classes").delete().eq("project_id", project_id).execute()
        supabase.table("frames").delete().eq("project_id", project_id).execute()
        supabase.table("uploads").delete().eq("project_id", project_id).execute()
        supabase.table("projects").delete().eq("id", project_id).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete project: {e}")

    return {"message": "Deleted"}