from fastapi import FastAPI
from db import Base,engine
from app import route
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
Base.metadata.create_all(bind=engine)
origins = [
    "http://localhost:5173",  
    "http://127.0.0.1:5173",
    "http://localhost:5174",
    "http://127.0.0.1:5174",  
    "http://localhost:3000",
    "http://localhost:8000",  
]
 
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,        
    allow_credentials=True,
    allow_methods=["*"],          
    allow_headers=["*"],        
)
app.include_router(route.router)
@app.get('/')
def greet():
    return 'Welcome!'