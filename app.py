from ddl_manager import DDLManager
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from query_manager import QueryManager
from storage_manager import StorageManager
import uvicorn

storage_manager = StorageManager()
query_manager = QueryManager()
ddl_manager = DDLManager(storage_manager)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class QueryRequest(BaseModel):
    query: str


@app.post("/query")
async def execute_query(data: QueryRequest):
    # TODO: add actual logic to excute parsed query (or somewhere else)
    # sample query
    # data.query = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))"

    try:
        query = query_manager.parse_query(data.query)
        return {"result": query}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
