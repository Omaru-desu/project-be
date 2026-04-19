from dotenv import load_dotenv
load_dotenv()

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.routes import upload
from app.api.routes.health import router as health_router
from app.projects import router as projects_router
from app.api.routes.labels import router as labels_router
from app.api.routes.segment import router as segment_router
from app.api.routes.embed import router as embed_router
from app.api.routes.status import router as status_router
from app.api.routes.preview import router as preview_router

cors_origins = os.getenv("CORS_ORIGINS", "http://localhost:3000")
allow_origins = [origin.strip() for origin in cors_origins.split(",") if origin.strip()]

app = FastAPI(title="fathom")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router, prefix="")
app.include_router(upload.router, prefix="/api")
app.include_router(projects_router, prefix="/api")
app.include_router(labels_router, prefix="/api")
app.include_router(segment_router, prefix="/api")
app.include_router(embed_router, prefix="/api")
app.include_router(status_router, prefix="/api")
app.include_router(preview_router, prefix="/api")