-- -- -- --  -- -- -- --
--   Initialisation   --
-- -- -- --  -- -- -- --

-- Création de la BDD linkedin
CREATE DATABASE IF NOT EXISTS linkedin;

-- On indique l'url du bucket s3 ou se situent les fichiers
create stage linkedin_jobs url = 's3://snowflake-lab-bucket/';

-- On créé 2 format (csv et json) afin d'ajouter les fichiers du bucket aux tables de la DB
create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ','
  record_delimiter = '\n'  skip_header = 1
  field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none'
  escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto'
  null_if = ('') comment = 'file format for ingesting csv';
--
CREATE OR REPLACE FILE FORMAT json_format TYPE = JSON;
-- -- -- -- -- -- -- --
-- -- -- -- -- -- -- --


-- -- -- --   -- -- -- --
-- Création des tables --
-- -- -- --   -- -- -- --
-- jobs_posting
CREATE TABLE jobs_posting (
    job_id VARCHAR PRIMARY KEY,
    company_id VARCHAR,
    title VARCHAR,
    description VARCHAR,
    max_salary VARCHAR,
    med_salary VARCHAR,
    min_salary VARCHAR,
    pay_period VARCHAR,
    formatted_work_type VARCHAR,
    location VARCHAR,
    applies VARCHAR,
    original_listed_time VARCHAR,
    remote_allowed VARCHAR,
    views VARCHAR,
    job_posting_url VARCHAR,
    application_url VARCHAR,
    application_type VARCHAR,
    expiry VARCHAR,
    closed_time VARCHAR,
    formatted_experience_level VARCHAR,
    skills_desc VARCHAR,
    listed_time VARCHAR,
    posting_domain VARCHAR,
    sponsored VARCHAR,
    work_type VARCHAR,
    currency VARCHAR,
    compensation_type VARCHAR,
    scraped VARCHAR
);
-- salaries
CREATE TABLE salaries (
    salary_id VARCHAR PRIMARY KEY,
    job_id VARCHAR REFERENCES Jobs_posting(job_id),
    max_salary VARCHAR,
    med_salary VARCHAR,
    min_salary VARCHAR,
    pay_period VARCHAR,
    currency VARCHAR,
    compensation_type VARCHAR
);
-- benefits
CREATE TABLE benefits (
    job_id VARCHAR,
    type VARCHAR,
    inferred VARCHAR
);
-- companies
CREATE TABLE companies (
    company_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    description VARCHAR,
    company_size VARCHAR,
    country VARCHAR,
    state VARCHAR,
    city VARCHAR,
    zip_code VARCHAR,
    address VARCHAR,
    url VARCHAR
);
-- skills
CREATE TABLE skills (
    skill_abr VARCHAR PRIMARY KEY,
    skill_name VARCHAR
);
-- employee_counts
CREATE TABLE employee_counts (
    company_id VARCHAR,
    employee_count VARCHAR,
    follower_count VARCHAR,
    time_recorded VARCHAR
);
-- job_skills
CREATE TABLE job_skills (
    job_id VARCHAR PRIMARY KEY REFERENCES Jobs_posting(job_id),
    skill_abr VARCHAR
);
-- industries
CREATE TABLE industries (
    industry_id VARCHAR PRIMARY KEY,
    industry_name VARCHAR
);
-- job_industries
CREATE TABLE job_industries (
    job_id VARCHAR REFERENCES Jobs_posting(job_id) PRIMARY KEY,
    industry_id VARCHAR REFERENCES industries(industry_id)
);
-- company_specialities
CREATE TABLE company_specialities (
    company_id VARCHAR REFERENCES companies(company_id) PRIMARY KEY,
    speciality VARCHAR
);
-- company_industries
CREATE TABLE company_industries (
    company_id VARCHAR REFERENCES companies(company_id) PRIMARY KEY,
    industry VARCHAR
);

-- Cette commande nous permet de voir le contenu de notre stage linkedin_jobs et de connaître
--      le type des fichiers que nous allons transférer dans nos tables.
list @linkedin_jobs;

-- Création des tables temporaires qui serviront à ingérer les données proprement --
-- tables pour les docs json
CREATE TABLE IF NOT EXISTS json_benefits (v variant);
CREATE TABLE IF NOT EXISTS json_company_industries (v variant);
CREATE TABLE IF NOT EXISTS json_employee_counts (v variant);
CREATE TABLE IF NOT EXISTS json_industries (v variant);
CREATE TABLE IF NOT EXISTS json_job_industries (v variant);
CREATE TABLE IF NOT EXISTS json_job_skills (v variant);
CREATE TABLE IF NOT EXISTS json_salaries (v variant);
CREATE TABLE IF NOT EXISTS json_skills (v variant);

-- tables pour les docs csv
CREATE TABLE IF NOT EXISTS csv_companies (
    company_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    description VARCHAR,
    company_size VARCHAR,
    country VARCHAR,
    state VARCHAR,
    city VARCHAR,
    zip_code VARCHAR,
    address VARCHAR,
    url VARCHAR
);
--
CREATE TABLE IF NOT EXISTS csv_company_specialities (
    company_id VARCHAR REFERENCES companies(company_id) PRIMARY KEY,
    speciality VARCHAR
);
--
CREATE TABLE IF NOT EXISTS csv_jobs_posting (
    job_id VARCHAR PRIMARY KEY,
    company_id VARCHAR,
    title VARCHAR,
    description VARCHAR,
    max_salary VARCHAR,
    med_salary VARCHAR,
    min_salary VARCHAR,
    pay_period VARCHAR,
    formatted_work_type VARCHAR,
    location VARCHAR,
    applies VARCHAR,
    original_listed_time VARCHAR,
    remote_allowed VARCHAR,
    views VARCHAR,
    job_posting_url VARCHAR,
    application_url VARCHAR,
    application_type VARCHAR,
    expiry VARCHAR,
    closed_time VARCHAR,
    formatted_experience_level VARCHAR,
    skills_desc VARCHAR,
    listed_time VARCHAR,
    posting_domain VARCHAR,
    sponsored VARCHAR,
    work_type VARCHAR,
    currency VARCHAR,
    compensation_type VARCHAR,
    scraped VARCHAR
);
-- -- -- -- -- -- -- --
-- -- -- -- -- -- -- --



-- -- -- --     -- -- -- --
-- Insertion des données --
-- -- -- --     -- -- -- --
-- Copie des données des fichiers json dans les tables temporaires --
COPY INTO json_benefits
    FROM @linkedin_jobs/benefits.json
    FILE_FORMAT = (FORMAT_NAME = 'json_format');
--
COPY INTO json_company_industries
    FROM @linkedin_jobs/company_industries.json
    FILE_FORMAT = (FORMAT_NAME = 'json_format');
--
COPY INTO json_employee_counts
    FROM @linkedin_jobs/employee_counts.json
    FILE_FORMAT = (FORMAT_NAME = 'json_format');
--
COPY INTO json_industries
    FROM @linkedin_jobs/industries.json
    FILE_FORMAT = (FORMAT_NAME = 'json_format');
--
COPY INTO json_job_industries
    FROM @linkedin_jobs/job_industries.json
    FILE_FORMAT = (FORMAT_NAME = 'json_format');
--
COPY INTO json_job_skills
    FROM @linkedin_jobs/job_skills.json
    FILE_FORMAT = (FORMAT_NAME = 'json_format');
--
COPY INTO json_salaries
    FROM @linkedin_jobs/salaries.json
    FILE_FORMAT = (FORMAT_NAME = 'json_format');
--
COPY INTO json_skills
    FROM @linkedin_jobs/skills.json
    FILE_FORMAT = (FORMAT_NAME = 'json_format'); 

-- Copie des données des fichiers csv dans les tables temporaires --
--
copy into csv_companies 
    FROM @linkedin_jobs
    FILE_FORMAT = (FORMAT_NAME = 'csv')
    FILES = ('companies.csv');
--
copy into csv_company_specialities 
    FROM @linkedin_jobs
    FILE_FORMAT = (FORMAT_NAME = 'csv')
    FILES = ('company_specialities.csv');
--
copy into csv_jobs_posting 
    FROM @linkedin_jobs
    FILE_FORMAT = (FORMAT_NAME = 'csv')
    FILES = ('job_postings.csv');


-- Transfère des données (issu des fichiers json) des tables temporaires dans les  
--      tables finales avec ajustement des types des données
--
INSERT INTO benefits (inferred, job_id, type)
    SELECT
        value:inferred::INT AS inferred,
        value:job_id::INT AS job_id,
        value:type::STRING AS type
    FROM json_benefits,
    TABLE(FLATTEN(input => v)) ;
--
INSERT INTO company_industries (company_id, industry)
    SELECT 
        value:company_id::INT AS company_id,
        value:industry::VARCHAR AS industry
    FROM json_company_industries,
    TABLE(FLATTEN(input => v));
--
INSERT INTO employee_counts(company_id, employee_count, follower_count, time_recorded)
    SELECT 
        value:company_id::INT AS company_id,
        value:employee_count::INT AS employee_count,
        value:follower_count::INT AS follower_count,
        value:time_recorded::INT AS time_recorded
    FROM json_employee_counts,
    TABLE(FLATTEN(input => v));
--
INSERT INTO skills (skill_abr, skill_name)
    SELECT
        value:skill_abr::VARCHAR AS skill_abr,
        value:skill_name::VARCHAR AS skill_name
    FROM json_skills,
    TABLE (FLATTEN(input => v));
--
INSERT INTO salaries (salary_id, job_id, max_salary, med_salary, min_salary, pay_period, currency, compensation_type)
    SELECT
        value:salary_id::VARCHAR AS salary_id,
        value:job_id::VARCHAR AS job_id,
        value:max_salary::FLOAT AS max_salary,
        value:med_salary::FLOAT AS med_salary,
        value:min_salary::FLOAT AS min_salary,
        value:pay_period::VARCHAR AS pay_period,
        value:currency::VARCHAR AS currency,
        value:compensation_type::VARCHAR AS compensation_type
    FROM json_salaries,
    TABLE (FLATTEN(input => v));
--
INSERT INTO job_skills (job_id, skill_abr)
    SELECT
        value:job_id::VARCHAR AS job_id,
        value:skill_abr::VARCHAR AS skill_abr
    FROM json_job_skills,
    TABLE(FLATTEN(input => v));
--
INSERT INTO job_industries (job_id, industry_id)
    SELECT
        value:job_id::VARCHAR AS job_id,
        value:industry_id::VARCHAR AS industry_id
    FROM json_job_industries,
    TABLE (FLATTEN(input => v));
--
INSERT INTO industries (industry_id, industry_name)
    SELECT
        value:industry_id::VARCHAR AS industry_id,
        value:industry_name::VARCHAR AS industry_name
    FROM json_industries,
    TABLE (FLATTEN(input => v));

-- Transfère des données (issu des fichiers csv) des tables temporaires dans les  
--      tables finales avec ajustement des types des données
INSERT INTO company_specialities (
  company_id,
  speciality
)
SELECT
  CAST(company_id AS INT),
  CAST(speciality AS VARCHAR)
FROM csv_company_specialities;
--
INSERT INTO companies (
  company_id,
  name,
  description,
  company_size,
  country,
  state,
  city,
  zip_code,
  address,
  url
)
SELECT
  CAST(company_id AS INT),
  CAST(name AS VARCHAR),
  CAST(description AS VARCHAR),
  CAST(company_size AS INT),  
  CAST(country AS VARCHAR),
  CAST(state AS VARCHAR),
  CAST(city AS VARCHAR),
  CAST(zip_code AS VARCHAR),
  CAST(address AS VARCHAR),
  CAST(url AS VARCHAR)
FROM csv_companies;
--
INSERT INTO jobs_posting (
  job_id,
  company_id,
  title,
  description,
  max_salary,
  med_salary,
  min_salary,
  pay_period,
  formatted_work_type,
  location,
  applies,
  original_listed_time,
  remote_allowed,
  views,
  job_posting_url,
  application_url,
  application_type,
  expiry,
  closed_time,
  formatted_experience_level,
  skills_desc,
  listed_time,
  posting_domain,
  sponsored,
  work_type,
  currency,
  compensation_type,
  scraped
)
SELECT
  CAST(job_id AS INT),
  CAST(company_id AS INT),
  CAST(title AS VARCHAR),
  CAST(description AS VARCHAR),
  CAST(max_salary AS FLOAT),
  CAST(med_salary AS FLOAT),  
  CAST(min_salary AS FLOAT),
  CAST(pay_period AS VARCHAR),
  CAST(formatted_work_type AS VARCHAR),
  CAST(location AS VARCHAR),
  CAST(applies AS INT),
  CAST(original_listed_time AS FLOAT),
  CAST(remote_allowed AS BOOLEAN),
  CAST(views AS INT),
  CAST(job_posting_url AS VARCHAR),
  CAST(application_url AS VARCHAR),
  CAST(application_type AS VARCHAR),
  CAST(expiry AS FLOAT),  
  CAST(closed_time AS FLOAT),
  CAST(formatted_experience_level AS VARCHAR),
  CAST(skills_desc AS VARCHAR),
  CAST(listed_time AS FLOAT),
  CAST(posting_domain AS VARCHAR),
  CAST(sponsored AS VARCHAR),
  CAST(work_type AS VARCHAR),
  CAST(currency AS VARCHAR),
  CAST(compensation_type AS VARCHAR),
  CAST(scraped AS FLOAT)
FROM csv_jobs_posting;
-- -- -- -- -- -- -- --
-- -- -- -- -- -- -- --



-- -- -- -- -- -- -- --
--   Requêtage SQL   --
-- -- -- -- -- -- -- --

-- Requête 1
SELECT
    title, COUNT(*) AS count
FROM
    jobs_posting
GROUP BY
    title
ORDER BY
    count DESC
LIMIT 10;

-- Requête 2
SELECT
    title, COALESCE(MAX(max_salary),0) AS highest_salary
FROM
    jobs_posting
WHERE
    currency = 'USD'
GROUP BY
    title
ORDER BY
    highest_salary DESC
LIMIT
    1;  

-- Requête 3
SELECT
    companies.company_size, COUNT(*) AS job_count
FROM
    jobs_posting
JOIN
    companies
    ON jobs_posting.company_id = companies.company_id
GROUP BY
    companies.company_size;

-- Requête 4
SELECT
    industries.industry_name, COUNT(*) AS job_count
FROM
    jobs_posting
JOIN
    job_industries ON jobs_posting.job_id = job_industries.job_id
JOIN
    industries ON job_industries.industry_id = industries.industry_id
GROUP BY
    industries.industry_name; 

-- Requête 5
SELECT
    formatted_work_type, COUNT(*) AS job_count
FROM
    jobs_posting
GROUP BY
    formatted_work_type;
-- -- -- -- -- -- -- --
-- -- -- -- -- -- -- --
