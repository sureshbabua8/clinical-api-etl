from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Dict, Any
import uvicorn
import os
import logging
from src.etl_processor import ETLProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Clinical Data ETL Service", version="1.0.0")
etl_processor = ETLProcessor()

jobs: Dict[str, Dict[str, Any]] = {}

class ETLJobRequest(BaseModel):
    jobId: str
    filename: str
    studyId: str

class ETLJobResponse(BaseModel):
    jobId: str
    status: str
    message: str

class ETLJobStatus(BaseModel):
    jobId: str
    status: str
    progress: Optional[int] = None
    message: Optional[str] = None

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "etl"}

async def process_etl_job(job_id: str, filename: str, study_id: str):
    """
    Background task to process ETL job

    This runs asynchronously to avoid blocking the API response
    """
    try:
        logger.info(f"Processing ETL job {job_id}")

        jobs[job_id]["status"] = "running"
        jobs[job_id]["progress"] = 10
        jobs[job_id]["message"] = "Processing file..."

        result = await etl_processor.process_job(job_id, filename, study_id)

        jobs[job_id]["status"] = result["status"]
        jobs[job_id]["progress"] = 100 if result["status"] == "completed" else 0
        jobs[job_id]["message"] = result.get("message", "")
        jobs[job_id]["result"] = result

        logger.info(f"ETL job {job_id} completed with status: {result['status']}")

    except Exception as e:
        logger.error(f"Error processing job {job_id}: {e}")
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["progress"] = 0
        jobs[job_id]["message"] = f"Processing failed: {str(e)}"


@app.post("/jobs", response_model=ETLJobResponse)
async def submit_job(job_request: ETLJobRequest, background_tasks: BackgroundTasks):
    """
    Submit a new ETL job for processing

    The job will be processed asynchronously in the background
    """
    job_id = job_request.jobId

    jobs[job_id] = {
        "jobId": job_id,
        "filename": job_request.filename,
        "studyId": job_request.studyId,
        "status": "pending",
        "progress": 0,
        "message": "Job queued for processing"
    }

    background_tasks.add_task(
        process_etl_job,
        job_id,
        job_request.filename,
        job_request.studyId
    )

    logger.info(f"Job {job_id} submitted for processing")

    return ETLJobResponse(
        jobId=job_id,
        status="pending",
        message="Job submitted successfully and queued for processing"
    )

@app.get("/jobs/{job_id}/status", response_model=ETLJobStatus)
async def get_job_status(job_id: str):
    """
    Get the current status of an ETL job
    """
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    return ETLJobStatus(
        jobId=job_id,
        status=job["status"],
        progress=job.get("progress"),
        message=job.get("message")
    )

@app.get("/jobs/{job_id}")
async def get_job_details(job_id: str):
    """
    Get detailed information about an ETL job
    """
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return jobs[job_id]

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
