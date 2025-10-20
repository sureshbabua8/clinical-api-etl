import psycopg2
from psycopg2.extras import execute_batch
import os
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class DatabaseService:
    """Database service for clinical data ETL operations"""

    def __init__(self):
        self.connection_string = os.getenv(
            "DATABASE_URL",
            "postgresql://user:pass@postgres:5432/clinical_data"
        )

    def get_connection(self):
        """Get database connection"""
        try:
            conn = psycopg2.connect(self.connection_string)
            return conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise

    def insert_measurements(self, measurements: List[Dict[str, Any]]) -> int:
        """
        Insert clinical measurements into database

        Args:
            measurements: List of measurement dictionaries

        Returns:
            Number of rows inserted
        """
        if not measurements:
            return 0

        conn = self.get_connection()
        try:
            cursor = conn.cursor()

            insert_query = """
                INSERT INTO clinical_measurements
                (study_id, participant_id, measurement_type, value, unit,
                 timestamp, site_id, quality_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            values = [
                (
                    m['study_id'],
                    m['participant_id'],
                    m['measurement_type'],
                    m['value'],
                    m['unit'],
                    m['timestamp'],
                    m['site_id'],
                    m['quality_score']
                )
                for m in measurements
            ]

            execute_batch(cursor, insert_query, values)
            conn.commit()

            rows_inserted = cursor.rowcount
            logger.info(f"Inserted {rows_inserted} measurements into database")

            return rows_inserted

        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting measurements: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def update_job_status(self, job_id: str, status: str,
                         progress: int = None, message: str = None):
        """Update ETL job status in database"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()

            # Note: This updates the in-memory jobs dict in main.py
            # For production, you'd update the etl_jobs table here

            cursor.close()
            conn.commit()

        except Exception as e:
            conn.rollback()
            logger.error(f"Error updating job status: {e}")
            raise
        finally:
            conn.close()
