USE ROLE sysadmin;

-- assign Query Tag to Session 
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"ss_zts","version":{"major":1, "minor":1},"attributes":{"is_quickstart":1, "source":"sql", "vignette": "intro"}}';

/*--
 • database, schema and warehouse creation
--*/

-- create ss_101 database
CREATE  DATABASE IF NOT EXISTS ss_101;


-- create harmonized schema
CREATE OR REPLACE SCHEMA ss_101.harmonized;

-- create analytics schema
CREATE OR REPLACE SCHEMA ss_101.analytics;

-- create warehouses
CREATE OR REPLACE WAREHOUSE ss_de_wh
    WAREHOUSE_SIZE = 'large' -- Large for initial data load - scaled down to XSmall at end of this scripts
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'data engineering warehouse for summit sports';

CREATE OR REPLACE WAREHOUSE ss_dev_wh
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'developer warehouse for summit sports';

-- create roles
USE ROLE securityadmin;

-- functional roles
CREATE ROLE IF NOT EXISTS ss_admin
    COMMENT = 'admin for summit sports';
    
CREATE ROLE IF NOT EXISTS ss_data_engineer
    COMMENT = 'data engineer for summit sports';
    
CREATE ROLE IF NOT EXISTS ss_dev
    COMMENT = 'developer for summit sports';
    
-- role hierarchy
GRANT ROLE ss_admin TO ROLE sysadmin;
GRANT ROLE ss_data_engineer TO ROLE ss_admin;
GRANT ROLE ss_dev TO ROLE ss_data_engineer;

-- privilege grants
USE ROLE accountadmin;

GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE ss_data_engineer;

GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE ss_admin;

USE ROLE securityadmin;

GRANT ALL ON DATABASE ss_101 TO ROLE ss_admin;
GRANT USAGE ON DATABASE ss_101 TO ROLE ss_data_engineer;
GRANT USAGE ON DATABASE ss_101 TO ROLE ss_dev;

GRANT ALL ON ALL SCHEMAS IN DATABASE ss_101 TO ROLE ss_admin;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ss_101 TO ROLE ss_data_engineer;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ss_101 TO ROLE ss_dev;

GRANT ALL ON SCHEMA ss_101.harmonized TO ROLE ss_admin;
GRANT ALL ON SCHEMA ss_101.harmonized TO ROLE ss_data_engineer;
GRANT ALL ON SCHEMA ss_101.harmonized TO ROLE ss_dev;

GRANT ALL ON SCHEMA ss_101.analytics TO ROLE ss_admin;
GRANT ALL ON SCHEMA ss_101.analytics TO ROLE ss_data_engineer;
GRANT ALL ON SCHEMA ss_101.analytics TO ROLE ss_dev;

GRANT IMPORTED PRIVILEGES ON DATABASE ss_raw TO ROLE ss_admin;
GRANT IMPORTED PRIVILEGES ON DATABASE ss_raw TO ROLE ss_data_engineer;
GRANT IMPORTED PRIVILEGES ON DATABASE ss_raw TO ROLE ss_dev;

-- warehouse grants
GRANT OWNERSHIP ON WAREHOUSE ss_de_wh TO ROLE ss_admin COPY CURRENT GRANTS;
GRANT ALL ON WAREHOUSE ss_de_wh TO ROLE ss_admin;
GRANT ALL ON WAREHOUSE ss_de_wh TO ROLE ss_data_engineer;

GRANT ALL ON WAREHOUSE ss_dev_wh TO ROLE ss_admin;
GRANT ALL ON WAREHOUSE ss_dev_wh TO ROLE ss_data_engineer;
GRANT ALL ON WAREHOUSE ss_dev_wh TO ROLE ss_dev;

-- future grants

GRANT ALL ON FUTURE VIEWS IN SCHEMA ss_101.harmonized TO ROLE ss_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA ss_101.harmonized TO ROLE ss_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA ss_101.harmonized TO ROLE ss_dev;

GRANT ALL ON FUTURE VIEWS IN SCHEMA ss_101.analytics TO ROLE ss_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA ss_101.analytics TO ROLE ss_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA ss_101.analytics TO ROLE ss_dev;



