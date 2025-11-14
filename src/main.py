from fastapi import FastAPI

app = FastAPI(title="CELINE API Prototype")

@app.get("/")
async def root():
    return {"message": "Hello from CELINE FastAPI container!"}

@app.get("/health")
async def health():
    return {"status": "healthy"}
