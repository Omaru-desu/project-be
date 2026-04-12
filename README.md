# Omaru

- OmarK2804 - Omar Khalif Muchzi
s4905768@student.uq.edu.au
- niboobin - Muhammad Obin Mandalika
s4905859@student.uq.edu.au

# REST API Backend
This is the backend of the project built using REST API principles with FastAPI. It handles data processing, API endpoints, and integration with Supabase as the database.

## 1. Clone Repository
```
git clone <repo-url>
```

## 2. Create Virtual Environment
```
# Mac/Linux: 
python -m venv env
source env/bin/activate   

# Windows:
env\Scripts\activate
```

## 3. Install Dependencies
```
pip install -r requirements.txt
```

## 4. Configure Environment
Create a .env file in the root directory:
```
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
```

## 5. Run Server
```
uvicorn main:app --reload
```

## 6. Access API
Go to http://localhost:8000/docs
