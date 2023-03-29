# End-To-End ELT Pipeline for Customer Intelligence

Automated pipeline that scrapes product reviews and metadata for competing Amazon products daily, then validates, transforms, and summarizes data in a dashboard.

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

1. Docker / Docker-Compose
2. AWS CLI

## Setup

1. `sh build.sh`: Creates extended Airflow Docker image, builds and runs Docker container, runs pipeline.
2. `sh shutdown.sh`: Shuts down pipeline and Docker containers.

## TODOS

0. Refactoring
1. Tweak dashboard views
2. Terraform for infra provisioning
3. Makefile for easy setup