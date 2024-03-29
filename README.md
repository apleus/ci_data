# ELT Pipeline for Customer Intelligence

Automated pipeline that scrapes product reviews and metadata for competing Amazon products daily, then validates, transforms, and summarizes data in a dashboard.

Products shown in example dashboard are (1) Keurig K-Supreme, (2) Nespresso VertuoNext, and (3) Nespresso VertuoPlus. These competing products were scraped for illustrative purposes to demonstrate pipeline functionality.

## Dashboard

![Dashboard1](docs/dashboard-1.png)
![Dashboard2](docs/dashboard-2.png)

## Pipeline Architecture

![Architecture](docs/architecture_diagram.jpg)

Pipeline flow:
1. Scrape "raw" data from [specified Amazon products](tasks/products.txt) and upload to S3.
2. Sanitize and validate "raw" data using Pydantic to create "prep" data (stored in S3).
3. Load "prep" data into data warehouse (RDS).
4. Transform data within warehouse using dbt to create data marts / views.
5. Display views in Apache Superset dashboard.

Infrastructure also includes:
- Orchestration via Airflow.
- Containerization via Docker.

## Requirements

1. AWS S3, RDS (Add credentials to .env).
2. Docker (Required to run pipeline).
3. Apache Supeset (Setup and link data sources).

## Setup

1. `make build`: Creates extended Airflow Docker image.
2. `make up`: Setup Airflow Docker container network.

## TODOS

* Terraform for infra provisioning.