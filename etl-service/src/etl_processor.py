import pandas as pd
import os
from typing import Dict, Any, List
import logging
from datetime import datetime
from src.db import DatabaseService

logger = logging.getLogger(__name__)

class ETLProcessor:
    """ETL Processor for clinical data files"""

    def __init__(self):
        self.db_service = DatabaseService()
        self.data_directory = os.getenv("DATA_DIRECTORY", "/app/data")
        self.min_quality_score = 0.90  # Minimum acceptable quality score

    async def process_job(self, job_id: str, filename: str,
                         study_id: str) -> Dict[str, Any]:
        """
        Process ETL job

        Args:
            job_id: Unique job identifier
            filename: Name of the file to process
            study_id: study ID filter

        Returns:
            Processing results dictionary
        """
        try:
            logger.info(f"Starting ETL job {job_id} for file {filename}")

            # Step 1: Extract - Read CSV file
            df = self._extract_data(filename)
            logger.info(f"Extracted {len(df)} rows from {filename}")

            # Step 2: Transform - Clean and validate data
            df_transformed = self._transform_data(df, study_id)
            logger.info(f"Transformed data: {len(df_transformed)} valid rows")

            # Step 3: Quality Validation
            validation_results = self._validate_quality(df_transformed)
            logger.info(f"Quality validation: {validation_results}")

            # Step 4: Load - Insert into database
            records = df_transformed.to_dict('records')
            rows_inserted = self.db_service.insert_measurements(records)

            return {
                "status": "completed",
                "rows_processed": len(df),
                "rows_inserted": rows_inserted,
                "rows_rejected": len(df) - len(df_transformed),
                "validation_results": validation_results,
                "message": f"Successfully processed {rows_inserted} measurements"
            }

        except FileNotFoundError as e:
            logger.error(f"File not found: {filename}")
            return {
                "status": "failed",
                "error": f"File not found: {filename}",
                "message": str(e)
            }
        except Exception as e:
            logger.error(f"ETL processing error for job {job_id}: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "message": f"ETL processing failed: {str(e)}"
            }

    def _extract_data(self, filename: str) -> pd.DataFrame:
        """
        Extract data from CSV file

        Args:
            filename: Name of the CSV file

        Returns:
            DataFrame with extracted data
        """
        file_path = os.path.join(self.data_directory, filename)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {filename} not found at {file_path}")

        # Read CSV file
        df = pd.read_csv(file_path)

        # Validate required columns
        required_columns = [
            'study_id', 'participant_id', 'measurement_type',
            'value', 'unit', 'timestamp', 'site_id', 'quality_score'
        ]

        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        return df

    def _transform_data(self, df: pd.DataFrame,
                       study_id_filter: str) -> pd.DataFrame:
        """
        Transform and clean data

        Args:
            df: Input DataFrame
            study_id_filter: study ID to filter by

        Returns:
            Transformed DataFrame
        """
        df_clean = df.copy()

        # Filter by study ID
        if study_id_filter:
            df_clean = df_clean[df_clean['study_id'] == study_id_filter]

        # Remove rows with missing critical values
        df_clean = df_clean.dropna(subset=[
            'study_id', 'participant_id', 'measurement_type',
            'value', 'timestamp'
        ])

        # Convert timestamp to proper datetime format
        df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'])

        # Ensure quality_score is numeric
        df_clean['quality_score'] = pd.to_numeric(
            df_clean['quality_score'],
            errors='coerce'
        )

        # Fill missing units with empty string
        df_clean['unit'] = df_clean['unit'].fillna('')

        # Convert value to string (it can be numeric or text like "120/80")
        df_clean['value'] = df_clean['value'].astype(str)

        return df_clean

    def _validate_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate data quality

        Args:
            df: DataFrame to validate

        Returns:
            Dictionary with validation results
        """
        total_rows = len(df)

        if total_rows == 0:
            return {
                "total_rows": 0,
                "high_quality_rows": 0,
                "low_quality_rows": 0,
                "average_quality_score": 0.0,
                "quality_threshold": self.min_quality_score
            }

        # Check quality scores
        high_quality = df[df['quality_score'] >= self.min_quality_score]
        low_quality = df[df['quality_score'] < self.min_quality_score]

        avg_quality = df['quality_score'].mean()

        validation_results = {
            "total_rows": total_rows,
            "high_quality_rows": len(high_quality),
            "low_quality_rows": len(low_quality),
            "average_quality_score": round(float(avg_quality), 3),
            "quality_threshold": self.min_quality_score,
            "quality_issues": []
        }

        # Add warnings for low quality data
        if len(low_quality) > 0:
            validation_results["quality_issues"].append(
                f"{len(low_quality)} rows below quality threshold"
            )

        # Check for duplicate measurements
        duplicates = df.duplicated(
            subset=['study_id', 'participant_id', 'measurement_type', 'timestamp'],
            keep=False
        )
        if duplicates.any():
            validation_results["quality_issues"].append(
                f"{duplicates.sum()} duplicate measurements found"
            )

        return validation_results
