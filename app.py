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

class DatabaseConfig(BaseModel):
    host: str
    user: str
    password: str
    database: str = None

class JobDataConfig(BaseModel):
    job_keyword: str
    location_keyword: str
    time_limit: int


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


@app.post("/scrape_job_data")
async def scrape_job_data_api(config: JobDataConfig):
    try:
        job_data = scrape_job_data(config.job_keyword, config.location_keyword, config.time_limit)
        
        job_data_df = create_dataframe_of_job_data(job_data)
        
        save_job_data_dataframe_to_mysql(job_data_df)
        
        job_data_from_db = extract_job_data_from_DB()
        
        job_csv_file_path = save_job_data_to_csv(job_data_from_db)
        
        unique_job_df = get_unique_companies_df(job_data_df, "company_name")
        
        save_filtered_job_data_dataframe_to_mysql(unique_job_df)
        
        unique_job_data_from_db = extract_unique_job_data_from_db()
        
        unique_csv_file_path = save_unique_job_data_to_csv(unique_job_data_from_db)
        
        return {
            "message": "Job data scraped and saved successfully",
            "job_csv_file": job_csv_file_path.split('/')[-1],
            "unique_csv_file": unique_csv_file_path.split('/')[-1],
            "code": 200
        }
    except Exception as e:
        traceback_str = ''.join(traceback.format_tb(e.__traceback__))
        print(f"Exception: {e}\nTraceback: {traceback_str}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
