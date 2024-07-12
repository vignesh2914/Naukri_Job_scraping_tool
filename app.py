from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.responses import FileResponse
from src.exception import CustomException
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import traceback
from src.exception import CustomException
from src.database import create_database, create_tables
import os
import uvicorn
from src.Research import (
    scrape_job_data,
    create_dataframe_of_job_data,
    get_unique_companies_df,
    save_job_data_dataframe_to_mysql,
    save_filtered_job_data_dataframe_to_mysql,
    extract_job_data_from_DB,
    save_job_data_to_csv,
    extract_unique_job_data_from_db,
    save_unique_job_data_to_csv
)

app = FastAPI()



@app.post("/create_database")
async def create_database_api(config: DatabaseConfig):
    try:
        result = create_database(config.host, config.user, config.password)
        return {"status": "success", "message": "DB created successfully", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.post("/create_tables")
async def create_tables_api(config: DatabaseConfig):
    try:
        if not config.database:
            raise HTTPException(status_code=400, detail="Database name is required")
        result = create_tables(config.host, config.user, config.password, config.database)
        return {"status": "success", "message": "Tables created successfully", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal Server Error")
