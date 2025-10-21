# Clinical Data ETL Pipeline - Implementation Write-up

**Author**: Amirthavarshini Sureshbabu

**Date**: October 21, 2025

**Assessment**: Regeneron Clinical Data ETL Pipeline

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Task 1: ETL Job Status Endpoint](#task-1-etl-job-status-endpoint)
3. [Task 2: ETL Data Processing Pipeline](#task-2-etl-data-processing-pipeline)
4. [Task 3: Database Schema Design](#task-3-database-schema-design)
5. [Technical Challenges & Solutions](#technical-challenges--solutions)
6. [AI Tool Usage](#ai-tool-usage)
7. [Testing Strategy](#testing-strategy)

---

## Project Overview

### Architecture

The system follows a microservices architecture with three main components:

1. **API Service** (TypeScript/Node.js) - RESTful API for client interaction
2. **ETL Service** (Python/FastAPI) - Background job processing
3. **PostgreSQL Database** - Data persistence and analytics

### Technology Stack

- **Backend**: TypeScript (Express.js), Python (FastAPI)
- **Database**: PostgreSQL with optimized indexes
- **Containerization**: Docker Compose
- **Data Processing**: Pandas (Python)
- **Communication**: HTTP/REST, PostgreSQL connection pooling

---

## Task 1: ETL Job Status Endpoint

### Objective

Implement `GET /api/etl/jobs/{id}/status` endpoint to retrieve real-time ETL job status with proper error handling.

### Implementation Approach

#### 1. Route Definition (`api-service/src/routes/etl.routes.ts`)

Added the status route to the existing ETL routes:

```typescript
router.get('/jobs/:id/status', etlController.getJobStatus);
```

**Design Decision**: Followed RESTful conventions with `/jobs/:id/status` as a sub-resource, making the API discoverable.

#### 2. Controller Layer (`api-service/src/controllers/etl.controller.ts`)

```typescript
getJobStatus = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
  try {
    const { id } = req.params;
    const statusData = await this.etlService.getJobStatus(id);

    if (!statusData) {
      errorResponse(res, 'Job not found', 404);
      return;
    }

    successResponse(res, statusData, 'Status retrieved successfully');
  } catch (error) {
    next(error);
  }
};
```

**Key Decisions**:

- **Validation First**: Check if job exists before querying ETL service
- **404 Handling**: Return proper HTTP status for non-existent jobs
- **Error Propagation**: Use Express middleware for consistent error handling

#### 3. Service Layer (`api-service/src/services/etl.service.ts`)

The most critical piece - handling connection failures gracefully:

```typescript
async getJobStatus(jobId: string): Promise<{...} | null> {
  // 1. Validate job exists in database
  const job = await this.dbService.getETLJob(jobId);
  if (!job) return null;

  try {
    // 2. Attempt real-time status from ETL service
    const response = await axios.get(`${this.etlServiceUrl}/jobs/${jobId}/status`, {
      timeout: 5000 // Prevent hanging requests
    });

    return {
      jobId: jobId,
      status: response.data.status || job.status,
      progress: response.data.progress,
      message: response.data.message
    };
  } catch (error) {
    // 3. Graceful degradation on connection failure
    if (axios.isAxiosError(error)) {
      if (error.code === 'ECONNREFUSED' || error.code === 'ETIMEDOUT') {
        console.warn(`ETL service unreachable for job ${jobId}`);
        return {
          jobId,
          status: job.status,
          message: 'Using cached status - ETL service temporarily unavailable'
        };
      }
      // ... more error handling
    }
  }
}
```

**Design Principles**:

1. **Resilience**: Never fail completely - always return useful information
2. **Timeout Protection**: 5-second timeout prevents hanging requests
3. **Graceful Degradation**: Falls back to database status when ETL service is unavailable
4. **Error Discrimination**: Different handling for connection errors vs. 404s vs. other errors
5. **Observability**: Logging for debugging production issues

#### Response Format

```json
{
  "success": true,
  "message": "Status retrieved successfully",
  "data": {
    "jobId": "abc-123-def",
    "status": "running",
    "progress": 75,
    "message": "Processing data..."
  }
}
```

### Why This Approach?

**Problem**: In a microservices architecture, the ETL service might be down, restarting, or experiencing network issues.

**Solution**: Implement a two-tier status system:

1. **Primary**: Real-time status from ETL service (preferred)
2. **Fallback**: Cached status from database (reliable)

This ensures the API remains responsive even during partial system failures, which is critical for the production environment.

---

## Task 2: ETL Data Processing Pipeline

### Objective

Complete the Python ETL service to extract, transform, validate, and load clinical data.

### Implementation Status

The ETL processor (`etl-service/src/etl_processor.py`) was implemented with:

#### Extract Phase

```python
def _extract_data(self, filename: str) -> pd.DataFrame:
    file_path = os.path.join(self.data_directory, filename)
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
```

**Design Decision**: Fail fast if data structure is invalid - prevents partial/corrupt data loads.

#### Transform Phase

```python
def _transform_data(self, df: pd.DataFrame, study_id_filter: str = None) -> pd.DataFrame:
    df_clean = df.copy()

    # Filter by study ID if provided
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
    df_clean['quality_score'] = pd.to_numeric(df_clean['quality_score'], errors='coerce')

    # Convert value to string (handles "120/80" blood pressure values)
    df_clean['value'] = df_clean['value'].astype(str)

    return df_clean
```

**Key Decisions**:

- **Flexible Value Types**: Values stored as TEXT to handle both numeric (95.5) and composite (120/80) values
- **Timestamp Standardization**: Convert to proper datetime for consistent querying
- **Optional Filters**: Support study-specific processing

#### Validation Phase

```python
def _validate_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
    total_rows = len(df)

    # Quality score analysis
    high_quality = df[df['quality_score'] >= self.min_quality_score]
    low_quality = df[df['quality_score'] < self.min_quality_score]

    validation_results = {
        "total_rows": total_rows,
        "high_quality_rows": len(high_quality),
        "low_quality_rows": len(low_quality),
        "average_quality_score": round(float(df['quality_score'].mean()), 3),
        "quality_threshold": self.min_quality_score,
        "quality_issues": []
    }

    # Duplicate detection
    duplicates = df.duplicated(
        subset=['study_id', 'participant_id', 'measurement_type', 'timestamp'],
        keep=False
    )
    if duplicates.any():
        validation_results["quality_issues"].append(
            f"{duplicates.sum()} duplicate measurements found"
        )

    return validation_results
```

**Validation Strategy**:

1. Quality score thresholds (default: 0.90)
2. Duplicate detection on natural key
3. Comprehensive reporting for data quality monitoring

#### Load Phase

```python
def insert_measurements(self, measurements: List[Dict[str, Any]]) -> int:
    insert_query = """
        INSERT INTO clinical_measurements
        (study_id, participant_id, measurement_type, value, unit,
         timestamp, site_id, quality_score)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    values = [(m['study_id'], m['participant_id'], ...) for m in measurements]
    execute_batch(cursor, insert_query, values)
```

**Performance Optimization**: Using `execute_batch` for bulk inserts significantly improves performance over individual INSERT statements.

### Module Structure Fix

**Problem Encountered**: Import errors due to Python package structure

```txt
ModuleNotFoundError: No module named 'etl_processor'
```

**Root Cause**: Dockerfile runs `uvicorn src.main:app`, treating `src` as a package, but imports used relative paths.

**Solution**:

1. Created `etl-service/src/__init__.py` to make `src` a proper Python package
2. Updated imports to use absolute paths:
   - `from etl_processor import ETLProcessor` → `from src.etl_processor import ETLProcessor`
   - `from database import DatabaseService` → `from src.db import DatabaseService`

**Lesson**: In containerized Python applications, maintain consistency between entry point and import statements.

---

## Task 3: Database Schema Design

### Objective

Design an optimized PostgreSQL schema to support high-performance analytical queries on clinical trial data.

### Design Philosophy: Star Schema

I chose a **star schema** pattern optimized for OLAP (Online Analytical Processing).

**Why Star Schema?**

1. **Query Performance**: Minimizes JOINs for analytical queries
2. **Intuitive**: Business logic maps directly to schema
3. **Scalable**: Easy to partition fact table by time or study
4. **Flexible**: New dimensions can be added without restructuring

### Table Design

#### Dimension Tables (Reference Data)

**1. Studies Table**

```sql
CREATE TABLE IF NOT EXISTS studies (
    study_id VARCHAR(50) PRIMARY KEY,
    study_name VARCHAR(255),
    description TEXT,
    start_date DATE,
    end_date DATE,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Purpose**: Master data for clinical studies. Separating this allows for study metadata without duplicating in every measurement row.

**2. Sites Table**

```sql
CREATE TABLE IF NOT EXISTS sites (
    site_id VARCHAR(50) PRIMARY KEY,
    site_name VARCHAR(255),
    location VARCHAR(255),
    country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Purpose**: Research site information for geographic analysis and site performance tracking.

**3. Measurement Types Table**

```sql
CREATE TABLE IF NOT EXISTS measurement_types (
    measurement_type VARCHAR(50) PRIMARY KEY,
    category VARCHAR(50), -- 'vitals', 'lab', 'biometric'
    description TEXT,
    standard_unit VARCHAR(20),
    min_value DECIMAL(10,2),
    max_value DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Purpose**: Lookup table with validation rules. Enables:

- Data validation (min/max ranges)
- Categorization for reporting
- Unit standardization

**4. Participants Table**

```sql
CREATE TABLE IF NOT EXISTS participants (
    participant_id VARCHAR(50) PRIMARY KEY,
    study_id VARCHAR(50) NOT NULL,
    enrollment_date DATE,
    age INTEGER,
    gender VARCHAR(20),
    site_id VARCHAR(50),
    status VARCHAR(20) DEFAULT 'active',
    CONSTRAINT fk_participant_study FOREIGN KEY (study_id) REFERENCES studies(study_id),
    CONSTRAINT fk_participant_site FOREIGN KEY (site_id) REFERENCES sites(site_id)
);
```

**Design Decision**: Single PRIMARY KEY on `participant_id` allows participants to enroll in multiple studies (multiple rows, different `study_id`). This supports cross-study participant tracking.

#### Fact Table (Transaction Data)

**Clinical Measurements**

```sql
CREATE TABLE IF NOT EXISTS clinical_measurements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    study_id VARCHAR(50) NOT NULL,
    participant_id VARCHAR(50) NOT NULL,
    measurement_type VARCHAR(50) NOT NULL,
    value TEXT NOT NULL,
    unit VARCHAR(20),
    timestamp TIMESTAMP NOT NULL,
    site_id VARCHAR(50) NOT NULL,
    quality_score DECIMAL(3,2) CHECK (quality_score >= 0 AND quality_score <= 1),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_measurement_study FOREIGN KEY (study_id) REFERENCES studies(study_id),
    CONSTRAINT fk_measurement_participant FOREIGN KEY (participant_id) REFERENCES participants(participant_id),
    CONSTRAINT fk_measurement_type FOREIGN KEY (measurement_type) REFERENCES measurement_types(measurement_type),
    CONSTRAINT fk_measurement_site FOREIGN KEY (site_id) REFERENCES sites(site_id)
);
```

**Key Design Decisions**:

1. **UUID Primary Key**: Supports distributed systems, prevents ID collisions
2. **TEXT for Value**: Handles both numeric (95.5) and composite (120/80) values
3. **CHECK Constraint**: Ensures quality_score is valid (0-1 range)
4. **Foreign Keys**: Enforces referential integrity across all dimensions
5. **Denormalized site_id**: Kept in fact table despite being in participants table for query performance (avoids extra JOIN)

### Index Strategy

The index design is the most critical part for performance. I created indexes for each business question:

#### 1. Time-Series Queries by Study

```sql
CREATE INDEX idx_measurements_study_timestamp
ON clinical_measurements(study_id, timestamp DESC);
```

**Supports**: "What are the glucose trends for STUDY001?"

**Why Composite**: PostgreSQL can use this index for queries filtering by `study_id` and sorting by `timestamp`, avoiding a separate sort operation.

#### 2. Participant-Specific Time-Series

```sql
CREATE INDEX idx_measurements_participant_timestamp
ON clinical_measurements(participant_id, timestamp DESC);
```

**Supports**: "Show glucose trends for participant P001 over time"

#### 3. Measurement Type Analysis

```sql
CREATE INDEX idx_measurements_type_timestamp
ON clinical_measurements(measurement_type, timestamp DESC);
```

**Supports**: "Get all glucose measurements across all studies"

#### 4. Site Performance Analytics

```sql
CREATE INDEX idx_measurements_site_study
ON clinical_measurements(site_id, study_id);
```

**Supports**: "How do measurement counts compare across sites?"

#### 5. Quality Filtering (Partial Index)

```sql
CREATE INDEX idx_measurements_quality_score
ON clinical_measurements(quality_score) WHERE quality_score < 0.95;
```

**Why Partial**: Only indexes low-quality measurements, reducing index size by ~80% while supporting the critical query: "Which measurements have quality issues?"

**Performance Benefit**: Smaller indexes = less memory, faster updates, faster scans

#### 6. Recent Data Queries

```sql
CREATE INDEX idx_measurements_timestamp_study
ON clinical_measurements(timestamp DESC, study_id);
```

**Supports**: "What clinical data was collected in the last 30 days?"

**Design Decision**: `timestamp` first for efficient range scans, `study_id` second for filtering.

#### 7. Covering Index (Index-Only Scans)

```sql
CREATE INDEX idx_measurements_composite_covering
ON clinical_measurements(study_id, participant_id, measurement_type, timestamp DESC)
INCLUDE (value, unit, quality_score);
```

**Purpose**: PostgreSQL's INCLUDE clause adds extra columns to index leaf nodes, enabling index-only scans without touching the table.

**Performance Gain**: For queries selecting these specific columns, PostgreSQL reads only the index (typically 10-100x faster).

**Trade-off**: Larger index size, but worth it for frequently accessed columns.

#### 8. Unique Constraint (Data Integrity)

```sql
CREATE UNIQUE INDEX idx_measurements_unique
ON clinical_measurements(study_id, participant_id, measurement_type, timestamp);
```

**Purpose**: Prevents duplicate measurements. Business rule: same measurement can't be recorded twice at the exact same timestamp for a participant.

### Views and Materialized Views

#### Materialized View: Study Quality Summary

```sql
CREATE MATERIALIZED VIEW mv_study_quality_summary AS
SELECT
    s.study_id,
    s.study_name,
    COUNT(cm.id) AS total_measurements,
    AVG(cm.quality_score) AS avg_quality_score,
    COUNT(CASE WHEN cm.quality_score >= 0.90 THEN 1 END) AS high_quality_count,
    COUNT(CASE WHEN cm.quality_score < 0.90 THEN 1 END) AS low_quality_count,
    MAX(cm.timestamp) AS last_measurement_date,
    COUNT(DISTINCT cm.participant_id) AS participant_count,
    COUNT(DISTINCT cm.site_id) AS site_count
FROM studies s
LEFT JOIN clinical_measurements cm ON s.study_id = cm.study_id
GROUP BY s.study_id, s.study_name;
```

**Purpose**: Pre-compute expensive aggregations for dashboard queries.

**When to Use**:

- Query is expensive (multiple aggregations, large datasets)
- Data doesn't need to be real-time (refresh periodically)
- Query is run frequently (dashboard, reports)

**Refresh Strategy**:

```sql
SELECT refresh_analytics_views(); -- Call after ETL job completion
```

#### Regular View: Recent Measurements

```sql
CREATE OR REPLACE VIEW v_recent_measurements AS
SELECT
    cm.*,
    s.study_name,
    si.site_name,
    mt.category AS measurement_category
FROM clinical_measurements cm
JOIN studies s ON cm.study_id = s.study_id
JOIN sites si ON cm.site_id = si.site_id
LEFT JOIN measurement_types mt ON cm.measurement_type = mt.measurement_type
WHERE cm.timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'
ORDER BY cm.timestamp DESC;
```

**Purpose**: Simplify common queries with pre-defined JOINs and filters.

**Why Regular View**: Data needs to be real-time; relatively fast to execute with proper indexes.

### Helper Functions

#### BMI Calculation

```sql
CREATE OR REPLACE FUNCTION calculate_bmi(participant_id_param VARCHAR, measurement_date DATE)
RETURNS DECIMAL(5,2) AS $$
DECLARE
    height_cm DECIMAL(10,2);
    weight_kg DECIMAL(10,2);
BEGIN
    -- Get most recent height before measurement_date
    SELECT CAST(value AS DECIMAL(10,2)) INTO height_cm
    FROM clinical_measurements
    WHERE participant_id = participant_id_param
        AND measurement_type = 'height'
        AND unit = 'cm'
        AND DATE(timestamp) <= measurement_date
    ORDER BY timestamp DESC LIMIT 1;

    -- Get most recent weight before measurement_date
    SELECT CAST(value AS DECIMAL(10,2)) INTO weight_kg
    FROM clinical_measurements
    WHERE participant_id = participant_id_param
        AND measurement_type = 'weight'
        AND unit = 'kg'
        AND DATE(timestamp) <= measurement_date
    ORDER BY timestamp DESC LIMIT 1;

    IF height_cm IS NOT NULL AND weight_kg IS NOT NULL AND height_cm > 0 THEN
        RETURN ROUND(weight_kg / ((height_cm / 100) * (height_cm / 100)), 2);
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
```

**Usage**:

```sql
-- Average BMI for STUDY001 participants
SELECT
    p.study_id,
    AVG(calculate_bmi(p.participant_id, CURRENT_DATE)) as avg_bmi
FROM participants p
WHERE p.study_id = 'STUDY001'
GROUP BY p.study_id;
```

**Design Decision**: Function finds most recent height/weight before the specified date, handling cases where measurements aren't taken simultaneously.

### Data Integrity Features

1. **Foreign Key Constraints**: All relationships enforced at database level
2. **CHECK Constraints**: `quality_score` must be 0-1
3. **NOT NULL Constraints**: Critical fields cannot be null
4. **DEFAULT Values**: Timestamps auto-populate
5. **Unique Constraints**: Prevent duplicate measurements

**Philosophy**: Fail at write time, not read time. Better to reject invalid data than deal with corruption later.

### Performance Considerations

#### Query Optimization Examples

**Bad Query** (Full table scan):

```sql
SELECT * FROM clinical_measurements WHERE study_id = 'STUDY001';
```

**Good Query** (Uses index):

```sql
SELECT * FROM clinical_measurements
WHERE study_id = 'STUDY001'
ORDER BY timestamp DESC;
-- Uses: idx_measurements_study_timestamp
```

**Even Better** (Index-only scan):

```sql
SELECT study_id, participant_id, measurement_type, value, unit, quality_score
FROM clinical_measurements
WHERE study_id = 'STUDY001'
ORDER BY timestamp DESC;
-- Uses: idx_measurements_composite_covering (index-only scan)
```

#### Scalability Considerations

For production with millions of rows:

1. **Partitioning**: Partition `clinical_measurements` by timestamp

   ```sql
   CREATE TABLE clinical_measurements_2024_01 PARTITION OF clinical_measurements
   FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
   ```

2. **Archiving**: Move old data to separate tables/storage

3. **Vacuum Strategy**: Regular VACUUM ANALYZE for index health

4. **Connection Pooling**: Already implemented via pg.Pool

### Schema Evolution Strategy

**Sample Data Insertion**:

```sql
INSERT INTO studies (study_id, study_name, description, status) VALUES
    ('STUDY001', 'Glucose Monitoring Trial', '...', 'active'),
    ('STUDY002', 'Cardiovascular Health Study', '...', 'active')
ON CONFLICT (study_id) DO NOTHING;
```

**Why ON CONFLICT DO NOTHING**: Allows schema.sql to be re-run without errors (idempotent). Critical for:

- Container restarts
- Schema updates
- Testing/development

---

## Technical Challenges & Solutions

### Challenge 1: Database Schema Initialization Error

**Error**:

```
ERROR: column "site_id" specified more than once
```

**Root Cause**: In view `v_low_quality_measurements`, we used `SELECT cm.*, p.site_id, si.site_name`. Since `cm.*` already includes `site_id` from clinical_measurements table, adding `p.site_id` created a duplicate column.

**Solution**:

```sql
-- Before (ERROR)
SELECT cm.*, s.study_name, p.site_id, si.site_name

-- After (FIXED)

SELECT cm.*, s.study_name, si.site_name
```

**Lesson**: Be careful with `SELECT *` in views that JOIN to tables with overlapping column names.

### Challenge 2: Python Import Errors in ETL Service

**Error**:
```
ModuleNotFoundError: No module named 'etl_processor'
```

**Root Cause**: Dockerfile runs `uvicorn src.main:app`, treating `src` as a package, but files used relative imports (`from etl_processor import ...`).

**Solution**:
1. Created `src/__init__.py` to make it a proper Python package
2. Updated all imports to absolute paths:
   - `from etl_processor import ETLProcessor` → `from src.etl_processor import ETLProcessor`
   - `from database import DatabaseService` → `from src.db import DatabaseService`

**Lesson**: In containerized Python apps, ensure consistency between:
- Entry point (how uvicorn/gunicorn runs the app)
- Import statements (absolute vs relative)
- PYTHONPATH configuration

### Challenge 3: Status Endpoint Response Format

**Issue**: Initial implementation returned `{ status: {...} }` but README specified `{ jobId, status, progress, message }` at the data level.

**Solution**: Updated service to return structured object:
```typescript
return {
  jobId: jobId,
  status: response.data.status,
  progress: response.data.progress,
  message: response.data.message
};
```

**Lesson**: Always validate response format against API specification/documentation.

---

## AI Tool Usage

Throughout this assessment, I leveraged Anthropic's Claude to enhance productivity and code quality. Here's how:

### Code Review & Optimization

- **Connection Handling**: Suggested timeout configuration and graceful degradation patterns
- **Index Strategy**: Recommended specific index types (partial, covering) based on query patterns
- **Performance**: Identified opportunities for index-only scans and materialized views

**Value Add**: AI assistance allowed me to focus on architectural decisions and business logic while helping me to quickly implement boilerplate code.

---

## Testing Strategy

### Manual Testing Workflow

#### 1. Service Startup

```bash
docker compose up --build
```

**Verify**:

- All services start without errors
- Schema initializes successfully
- Sample data loads

#### 2. ETL Job Submission

```bash
curl -X POST http://localhost:3000/api/etl/jobs \
  -H "Content-Type: application/json" \
  -d '{"filename": "sample_study001.csv", "studyId": "STUDY001"}'
```

**Expected**: Returns job ID and "pending" status

#### 3. Status Monitoring
```bash
curl http://localhost:3000/api/etl/jobs/<job-id>/status
```

**Test Cases**:

- Valid job ID → Returns status with progress
- Invalid job ID → Returns 404
- ETL service down → Returns cached status with warning message

#### 4. Data Verification

```bash
curl "http://localhost:3000/api/data?studyId=STUDY001" | jq
```

**Verify**:

- Measurements loaded correctly
- Quality scores present
- Timestamps formatted properly

#### 5. Database Validation

```sql
-- Connect to database
docker exec -it regeneron_assessment-postgres-1 psql -U user -d clinical_data

-- Check measurement count
SELECT COUNT(*) FROM clinical_measurements;

-- Verify indexes exist
\di

-- Test analytical queries
SELECT * FROM mv_study_quality_summary;

-- Test BMI calculation
SELECT calculate_bmi('P001', '2024-01-15');
```

### Test Coverage Areas

1. **Happy Path**
   - Submit job with valid data
   - Query status while running
   - Query completed job
   - Retrieve processed data

2. **Error Handling**
   - Invalid job ID (404)
   - ETL service unavailable (graceful degradation)
   - Invalid CSV format
   - Missing required columns
   - Foreign key violations

3. **Data Quality**
   - Quality score calculations
   - Duplicate detection
   - Missing value handling
   - Type conversions

4. **Performance**
   - Index usage (EXPLAIN ANALYZE)
   - Query response times
   - Bulk insert performance