from fastapi import FastAPI
from app.api import api_router
from app.db.postgres import init_db, shutdown_db

app = FastAPI(title="CELINE API Prototype")

# Include routers
app.include_router(api_router, prefix="/celine")

@app.get("/")
async def root():
    return {"message": "Hello from CELINE FastAPI container!"}

@app.on_event("startup")
async def startup_event():
    await init_db()

@app.on_event("shutdown")
async def shutdown_event():
    await shutdown_db()
