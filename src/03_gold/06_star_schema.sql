-- 1. Create Dimension: Applicants
CREATE TABLE DIM_APPLICANT (
    applicant_key INT PRIMARY KEY IDENTITY(1,1),
    applicant_id VARCHAR(50),
    age INT,
    gender VARCHAR(10),
    annual_income DECIMAL(15, 2),
    location_state VARCHAR(50),
    employment_status VARCHAR(50)
);

-- 2. Create Dimension: Policies
CREATE TABLE DIM_POLICY (
    policy_key INT PRIMARY KEY IDENTITY(1,1),
    policy_id VARCHAR(50),
    policy_name VARCHAR(100),
    policy_type VARCHAR(50), -- e.g., Life, Health, Auto
    coverage_amount DECIMAL(15, 2),
    base_premium DECIMAL(10, 2)
);

-- 3. Create Dimension: Rules
CREATE TABLE DIM_RULES (
    rule_key INT PRIMARY KEY IDENTITY(1,1),
    rule_id VARCHAR(50),
    rule_name VARCHAR(100),
    rule_category VARCHAR(50), -- e.g., Age-based, Income-based
    rule_description TEXT
);

-- 4. Create Dimension: Date
CREATE TABLE DIM_DATE (
    date_key INT PRIMARY KEY, -- Format YYYYMMDD
    full_date DATE,
    day_name VARCHAR(10),
    month_name VARCHAR(10),
    calendar_year INT,
    quarter INT
);

-- 5. Create Fact Table: Eligibility Results
CREATE TABLE FACT_ELIGIBILITY_RESULTS (
    fact_id INT PRIMARY KEY IDENTITY(1,1),
    applicant_key INT REFERENCES DIM_APPLICANT(applicant_key),
    policy_key INT REFERENCES DIM_POLICY(policy_key),
    rule_key INT REFERENCES DIM_RULES(rule_key),
    date_key INT REFERENCES DIM_DATE(date_key),
    
    -- Measures (Facts)
    is_eligible BIT, -- 1 for True, 0 for False
    processing_time_ms INT,
    risk_score DECIMAL(5, 2),
    rule_failure_count INT
);

-- Move unique applicants from logs to Dimension
INSERT INTO DIM_APPLICANT (applicant_id, age, annual_income, location_state)
SELECT DISTINCT raw_app_id, app_age, app_income, app_state
FROM STAGING_ELIGIBILITY_LOGS
WHERE raw_app_id NOT IN (SELECT applicant_id FROM DIM_APPLICANT);

-- Move unique policies from logs to Dimension
INSERT INTO DIM_POLICY (policy_id, policy_name)
SELECT DISTINCT policy_code, policy_title
FROM STAGING_ELIGIBILITY_LOGS
WHERE policy_code NOT IN (SELECT policy_id FROM DIM_POLICY);

INSERT INTO FACT_ELIGIBILITY_RESULTS (
    applicant_key, 
    policy_key, 
    rule_key, 
    date_key, 
    is_eligible, 
    processing_time_ms
)
SELECT 
    a.applicant_key,
    p.policy_key,
    r.rule_key,
    CAST(CONVERT(VARCHAR(8), s.check_timestamp, 112) AS INT) as date_key, -- Converts Date to YYYYMMDD
    CASE WHEN s.eligibility_status = 'Passed' THEN 1 ELSE 0 END,
    s.processing_speed
FROM STAGING_ELIGIBILITY_LOGS s
-- Join to Dimensions to get the Surrogate Keys (The INT IDs)
JOIN DIM_APPLICANT a ON s.raw_app_id = a.applicant_id
JOIN DIM_POLICY p ON s.policy_code = p.policy_id
JOIN DIM_RULES r ON s.rule_triggered = r.rule_name;
