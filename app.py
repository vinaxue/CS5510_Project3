from ddl_manager import DDLManager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dml_manager import DMLManager
from query_manager import QueryManager
from storage_manager import StorageManager
import uvicorn


storage_manager = StorageManager(
    db_file="./data/sample_data.pkl", index_file="./data/sample_index.pkl"
)
ddl_manager = DDLManager(storage_manager)
dml_manager = DMLManager(storage_manager)
query_manager = QueryManager(storage_manager, ddl_manager, dml_manager)

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
    try:
        queries = data.query.split(";")
        last_res, total_runtime = None, 0
        for query in queries:
            if query.strip():  # Skip empty queries
                last_res, runtime = query_manager.execute_query(query.strip())
                total_runtime += runtime
        return {"result": last_res, "runtime": total_runtime}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail=str(e))
