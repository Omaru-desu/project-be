# Omarine

This is the backend of the project built using REST API principles with FastAPI. It handles data processing, API endpoints, and integration with Supabase as the database.

## Table of Contents

- [Tech Stack](#tech-stack)
- [Setup](#setup)

## Tech Stack

| Library | Purpose | Source |
|--------|--------|---------|
| FastAPI | REST API framework | https://fastapi.tiangolo.com |
| Uvicorn | ASGI server | https://www.uvicorn.org |
| Supabase Python | Database & auth client | https://github.com/supabase-community/supabase-py |
| Google Cloud Storage | GCS file storage | https://cloud.google.com/python/docs/reference/storage/latest |
| OpenCV | Image/video processing, frame extraction | https://github.com/opencv/opencv-python |
| Pillow | Image manipulation | https://python-pillow.org |
| httpx | Async HTTP client for model service calls | https://www.python-httpx.org |
| python-dotenv | Environment variable management | https://github.com/theskumar/python-dotenv |
| python-multipart | File upload handling | https://github.com/andrew-d/python-multipart |
| anyio | Async I/O support | https://anyio.readthedocs.io |


## Setup 
### 1. Clone Repository
```
git clone <repo-url>
```

### 2. Create Virtual Environment
Create a virtual environment:
```
python -m venv env
```
Activate it:
```
# Windows:
env\Scripts\activate.bat

# Mac/Linux:
source env/bin/activate
```
If successful, (env) will appear in the terminal.

### 3. Install Dependencies
```
pip install -r requirements.txt
```

### 4. Configure Environment
Create a .env file in the root directory:
```
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
SUPABASE_JWT_SECRET=your_supabase_jwt_secret
GOOGLE_APPLICATION_CREDENTIALS=path/to/your/gcp-service-account.json
MODEL_SERVICE_URL=https://your-modal-deployment-url
```

### 5. Run Server
```
uvicorn app.main:app --reload
```

### 6. Access API
Go to http://localhost:8000/docs
