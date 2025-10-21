-- Clinical Data ETL Pipeline Database Schema
-- Optimized schema with proper normalization, indexes, and constraints

-- ============================================================================
-- REFERENCE/DIMENSION TABLES
-- ============================================================================

-- Studies master table
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

-- Research sites table
CREATE TABLE IF NOT EXISTS sites (
    site_id VARCHAR(50) PRIMARY KEY,
    site_name VARCHAR(255),
    location VARCHAR(255),
    country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Measurement types lookup table
CREATE TABLE IF NOT EXISTS measurement_types (
    measurement_type VARCHAR(50) PRIMARY KEY,
    category VARCHAR(50),
    description TEXT,
    standard_unit VARCHAR(20),
    min_value DECIMAL(10,2),
    max_value DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Participants table
CREATE TABLE IF NOT EXISTS participants (
    participant_id VARCHAR(50) PRIMARY KEY,
    study_id VARCHAR(50) NOT NULL,
    enrollment_date DATE,
    age INTEGER,
    gender VARCHAR(20),
    site_id VARCHAR(50),
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_participant_study FOREIGN KEY (study_id) REFERENCES studies(study_id),
    CONSTRAINT fk_participant_site FOREIGN KEY (site_id) REFERENCES sites(site_id)
);

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- Clinical measurements (main fact table)
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

-- ETL Jobs tracking table
CREATE TABLE IF NOT EXISTS etl_jobs (
    id UUID PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    study_id VARCHAR(50),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    rows_processed INTEGER DEFAULT 0,
    rows_inserted INTEGER DEFAULT 0,
    rows_rejected INTEGER DEFAULT 0,
    CONSTRAINT fk_etl_job_study FOREIGN KEY (study_id) REFERENCES studies(study_id)
);

-- Data quality reports table
CREATE TABLE IF NOT EXISTS data_quality_reports (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    study_id VARCHAR(50) NOT NULL,
    report_date DATE NOT NULL,
    total_measurements INTEGER,
    high_quality_count INTEGER,
    low_quality_count INTEGER,
    average_quality_score DECIMAL(4,3),
    quality_threshold DECIMAL(3,2),
    duplicate_count INTEGER DEFAULT 0,
    missing_data_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_quality_report_study FOREIGN KEY (study_id) REFERENCES studies(study_id)
);

-- ============================================================================
-- INDEXES FOR ANALYTICAL QUERY OPTIMIZATION
-- ============================================================================

-- Indexes on clinical_measurements (most frequently queried table)

-- Composite index for study-based time-series queries
CREATE INDEX IF NOT EXISTS idx_measurements_study_timestamp
ON clinical_measurements(study_id, timestamp DESC);

-- Composite index for participant-specific queries
CREATE INDEX IF NOT EXISTS idx_measurements_participant_timestamp
ON clinical_measurements(participant_id, timestamp DESC);

-- Composite index for measurement type queries
CREATE INDEX IF NOT EXISTS idx_measurements_type_timestamp
ON clinical_measurements(measurement_type, timestamp DESC);

-- Composite index for site-based analytics
CREATE INDEX IF NOT EXISTS idx_measurements_site_study
ON clinical_measurements(site_id, study_id);

-- Index for quality filtering
-- Supports: "Which measurements have quality scores below our threshold?"
CREATE INDEX IF NOT EXISTS idx_measurements_quality_score
ON clinical_measurements(quality_score) WHERE quality_score < 0.95;

-- Composite index for recent data queries
CREATE INDEX IF NOT EXISTS idx_measurements_timestamp_study
ON clinical_measurements(timestamp DESC, study_id);

-- Covering index for common query patterns (includes frequently selected columns)
CREATE INDEX IF NOT EXISTS idx_measurements_composite_covering
ON clinical_measurements(study_id, participant_id, measurement_type, timestamp DESC)
INCLUDE (value, unit, quality_score);

-- Unique constraint to prevent duplicate measurements
CREATE UNIQUE INDEX IF NOT EXISTS idx_measurements_unique
ON clinical_measurements(study_id, participant_id, measurement_type, timestamp);

-- Indexes on participants table

-- Index for participant enrollment queries
CREATE INDEX IF NOT EXISTS idx_participants_study
ON participants(study_id) INCLUDE (status, enrollment_date);

-- Index for site-based participant queries
CREATE INDEX IF NOT EXISTS idx_participants_site
ON participants(site_id, study_id);

-- Indexes on etl_jobs table

-- Index for job status monitoring
CREATE INDEX IF NOT EXISTS idx_etl_jobs_status
ON etl_jobs(status, created_at DESC);

-- Index for study-specific job history
CREATE INDEX IF NOT EXISTS idx_etl_jobs_study
ON etl_jobs(study_id, created_at DESC);

-- Index for recent jobs
CREATE INDEX IF NOT EXISTS idx_etl_jobs_created_at
ON etl_jobs(created_at DESC);

-- Indexes on data_quality_reports table

-- Index for study quality trends
CREATE INDEX IF NOT EXISTS idx_quality_reports_study_date
ON data_quality_reports(study_id, report_date DESC);

-- ============================================================================
-- MATERIALIZED VIEWS FOR ANALYTICAL QUERIES
-- ============================================================================

-- View: Study quality summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_study_quality_summary AS
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_study_quality_summary
ON mv_study_quality_summary(study_id);

-- View: Site performance metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_site_performance AS
SELECT
    si.site_id,
    si.site_name,
    s.study_id,
    COUNT(cm.id) AS measurement_count,
    AVG(cm.quality_score) AS avg_quality_score,
    COUNT(DISTINCT cm.participant_id) AS participant_count,
    COUNT(DISTINCT cm.measurement_type) AS measurement_type_count,
    MIN(cm.timestamp) AS first_measurement,
    MAX(cm.timestamp) AS last_measurement
FROM sites si
CROSS JOIN studies s
LEFT JOIN clinical_measurements cm ON si.site_id = cm.site_id AND s.study_id = cm.study_id
GROUP BY si.site_id, si.site_name, s.study_id;

CREATE INDEX IF NOT EXISTS idx_mv_site_performance
ON mv_site_performance(site_id, study_id);

-- ============================================================================
-- REGULAR VIEWS FOR COMMON QUERIES
-- ============================================================================

-- View: Recent measurements (last 30 days)
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

-- View: Low quality measurements
CREATE OR REPLACE VIEW v_low_quality_measurements AS
SELECT
    cm.*,
    s.study_name,
    si.site_name
FROM clinical_measurements cm
JOIN studies s ON cm.study_id = s.study_id
JOIN participants p ON cm.participant_id = p.participant_id
JOIN sites si ON p.site_id = si.site_id
WHERE cm.quality_score < 0.90
ORDER BY cm.quality_score ASC, cm.timestamp DESC;

-- View: Participant enrollment summary
CREATE OR REPLACE VIEW v_participant_enrollment AS
SELECT
    s.study_id,
    s.study_name,
    COUNT(p.participant_id) AS total_participants,
    COUNT(CASE WHEN p.status = 'active' THEN 1 END) AS active_participants,
    COUNT(DISTINCT p.site_id) AS sites_with_participants,
    MIN(p.enrollment_date) AS first_enrollment,
    MAX(p.enrollment_date) AS last_enrollment
FROM studies s
LEFT JOIN participants p ON s.study_id = p.study_id
GROUP BY s.study_id, s.study_name;

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Function to refresh materialized views
CREATE OR REPLACE FUNCTION refresh_analytics_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_study_quality_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_site_performance;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate BMI from measurements
CREATE OR REPLACE FUNCTION calculate_bmi(participant_id_param VARCHAR, measurement_date DATE)
RETURNS DECIMAL(5,2) AS $$
DECLARE
    height_cm DECIMAL(10,2);
    weight_kg DECIMAL(10,2);
    bmi DECIMAL(5,2);
BEGIN
    -- Get most recent height and weight for the participant around the measurement date
    SELECT CAST(value AS DECIMAL(10,2)) INTO height_cm
    FROM clinical_measurements
    WHERE participant_id = participant_id_param
        AND measurement_type = 'height'
        AND unit = 'cm'
        AND DATE(timestamp) <= measurement_date
    ORDER BY timestamp DESC
    LIMIT 1;

    SELECT CAST(value AS DECIMAL(10,2)) INTO weight_kg
    FROM clinical_measurements
    WHERE participant_id = participant_id_param
        AND measurement_type = 'weight'
        AND unit = 'kg'
        AND DATE(timestamp) <= measurement_date
    ORDER BY timestamp DESC
    LIMIT 1;

    IF height_cm IS NOT NULL AND weight_kg IS NOT NULL AND height_cm > 0 THEN
        bmi := weight_kg / ((height_cm / 100) * (height_cm / 100));
        RETURN ROUND(bmi, 2);
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- SAMPLE DATA INSERTION (for testing)
-- ============================================================================

-- Insert sample studies
INSERT INTO studies (study_id, study_name, description, status) VALUES
    ('STUDY001', 'Glucose Monitoring Trial', 'Clinical trial for glucose and cholesterol monitoring', 'active'),
    ('STUDY002', 'Cardiovascular Health Study', 'Blood pressure and heart rate monitoring study', 'active')
ON CONFLICT (study_id) DO NOTHING;

-- Insert sample sites
INSERT INTO sites (site_id, site_name, location, country) VALUES
    ('SITE_A', 'Memorial Research Center', 'New York, NY', 'USA'),
    ('SITE_B', 'Regional Medical Institute', 'Boston, MA', 'USA')
ON CONFLICT (site_id) DO NOTHING;

-- Insert sample measurement types
INSERT INTO measurement_types (measurement_type, category, standard_unit, min_value, max_value) VALUES
    ('glucose', 'lab', 'mg/dL', 50, 300),
    ('cholesterol', 'lab', 'mg/dL', 100, 400),
    ('weight', 'biometric', 'kg', 30, 200),
    ('height', 'biometric', 'cm', 100, 250),
    ('blood_pressure', 'vitals', 'mmHg', NULL, NULL),
    ('heart_rate', 'vitals', 'bpm', 40, 200)
ON CONFLICT (measurement_type) DO NOTHING;

-- Insert sample participants
INSERT INTO participants (participant_id, study_id, site_id, enrollment_date, status) VALUES
    ('P001', 'STUDY001', 'SITE_A', '2024-01-01', 'active'),
    ('P002', 'STUDY001', 'SITE_A', '2024-01-02', 'active'),
    ('P003', 'STUDY001', 'SITE_A', '2024-01-03', 'active'),
    ('P001', 'STUDY002', 'SITE_B', '2024-01-05', 'active'),
    ('P002', 'STUDY002', 'SITE_B', '2024-01-06', 'active'),
    ('P003', 'STUDY002', 'SITE_B', '2024-01-07', 'active')
ON CONFLICT (participant_id) DO NOTHING;
