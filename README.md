# ci_data

End-to-end automated ELT pipeline that scrapes product reviews and metadata for competing Amazon products daily, then validates, transforms, and summarizes data in a dashboard.

## Dashboard

TODO: insert dashboard image

## Pipeline Architecture

TODO: insert diagram

DAG flow:
1. Scrape "raw" data from [specified Amazon products](products.txt) and upload to AWS S3
2. Sanitize and validate "raw" data using Pydantic to create "prep" data (stored in AWS S3)
3. Load "prep" data into data warehouse (AWS RDS)
4. Transform and test "prep" data in warehouse using dbt to create data marts / views

Infrastructure also includes:
- Orchestration via Airflow
- Containerization via Docker
- Compute via AWS EC2 (TODO)
- Dashboard via Apache Superset

## Requirements

## Setup



-- Other TODOs --

TODAY:
- fix simple style wins, mark more difficult as "TODO"
- integrate everything w/ airflow, dbt operators
- fix dashboard
- arch diagram
- make sure everythihng working in airflow / docker

NEXT:
- terraform / put everything onto AWS
    - created RDS schema manually
    - added data source and charts to superset manually
- makefile, etc.
- cleanup -- read entire codebase; organization? init.py? style? logging?
- documentation
- automate EC2, RDS to stop with action tied to budget alert...
- refactor...
    - should you update pipeline_metadata instead of adding new entry?
        - change key to just product_id, date

OPPORTUNITIES FOR EXTENSIONS:
- aggregate more product ids for deeper analyses across more products
- NLP / sentiment analysis on review text
- Change dimension tables to SCD2 (e.g. products table)

