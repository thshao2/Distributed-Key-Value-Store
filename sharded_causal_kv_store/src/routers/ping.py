from fastapi import APIRouter
from fastapi.responses import JSONResponse
from shared_data import SharedData

ping_router = APIRouter()

@ping_router.get("/ping")
def ping():
    return JSONResponse(content={"message": f"Node {SharedData.NODE_IDENTIFIER} is up."}, status_code=200)
