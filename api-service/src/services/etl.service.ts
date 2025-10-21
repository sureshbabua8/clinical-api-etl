import axios from 'axios';
import { v4 as uuidv4 } from 'uuid';
import { DatabaseService } from './database.service';

export interface ETLJob {
  id: string;
  filename: string;
  studyId?: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  createdAt: Date;
  updatedAt: Date;
  completedAt?: Date;
  errorMessage?: string;
}

export class ETLService {
  private dbService: DatabaseService;
  private etlServiceUrl: string;

  constructor() {
    this.dbService = new DatabaseService();
    this.etlServiceUrl = process.env.ETL_SERVICE_URL || 'http://etl:8000';
  }

  /**
   * Submit new ETL job
   */
  async submitJob(filename: string, studyId?: string): Promise<ETLJob> {
    const jobId = uuidv4();
    
    // Create job record in database
    const job: ETLJob = {
      id: jobId,
      filename,
      studyId,
      status: 'pending',
      createdAt: new Date(),
      updatedAt: new Date()
    };

    await this.dbService.createETLJob(job);

    // Submit job to ETL service
    try {
      await axios.post(`${this.etlServiceUrl}/jobs`, {
        jobId,
        filename,
        studyId
      });

      // Update job status to running
      await this.dbService.updateETLJobStatus(jobId, 'running');
      job.status = 'running';
    } catch (error) {
      // Update job status to failed
      await this.dbService.updateETLJobStatus(jobId, 'failed', 'Failed to submit to ETL service');
      job.status = 'failed';
      job.errorMessage = 'Failed to submit to ETL service';
    }

    return job;
  }

  /**
   * Get ETL job by ID
   */
  async getJob(jobId: string): Promise<ETLJob | null> {
    return await this.dbService.getETLJob(jobId);
  }

  // TODO: CANDIDATE TO IMPLEMENT
  // /**
  //  * Get ETL job status from ETL service
  //  */
  // async getJobStatus(jobId: string): Promise<{ status: string; progress?: number; message?: string }> {
  //   // Implementation needed:
  //   // 1. Validate jobId exists in database
  //   // 2. Call ETL service to get real-time status
  //   // 3. Handle connection errors gracefully
  //   // 4. Return formatted status response
  // }
  async getJobStatus(jobId: string): Promise<{ jobId: string; status: string; progress?: number; message?: string } | null> {
    // Validate job exists in database
    const job = await this.dbService.getETLJob(jobId);
    if (!job) {
      return null;
    }

    try {
      // Attempt to get real-time status from ETL service
      const response = await axios.get(`${this.etlServiceUrl}/jobs/${jobId}/status`, {
        timeout: 5000 // 5 second timeout
      });

      return {
        jobId: jobId,
        status: response.data.status || job.status,
        progress: response.data.progress,
        message: response.data.message
      };
    } catch (error) {
      // Handle connection errors gracefully
      if (axios.isAxiosError(error)) {
        if (error.code === 'ECONNREFUSED' || error.code === 'ETIMEDOUT') {
          // ETL service is down or unreachable
          console.warn(`ETL service unreachable for job ${jobId}: ${error.message}`);
          return {
            jobId: jobId,
            status: job.status,
            message: `Using cached status - ETL service temporarily unavailable`
          };
        } else if (error.response?.status === 404) {
          // Job not found in ETL service, return database status
          console.warn(`Job ${jobId} not found in ETL service, using database status`);
          return {
            jobId: jobId,
            status: job.status,
            message: 'Status from database'
          };
        }
      }

      // Generic error fallback
      console.error(`Error retrieving status for job ${jobId}:`, error);
      return {
        jobId: jobId,
        status: job.status,
        message: 'Unable to retrieve real-time status, showing last known status'
      };
    }
  }
}
