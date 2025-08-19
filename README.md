# Cloud Marketing Data Pipeline

## Overview
This project implements a **cloud-based data pipeline** that ingests, transforms, and loads marketing datasets from multiple sources into a centralized data warehouse. It demonstrates **data engineering best practices** using Python, Airflow, Snowflake, and Databricks.  

The goal is to build a reliable, scalable pipeline for marketing analytics teams to track campaign performance, user engagement, and ROI.

---

## Features
- **Data Ingestion**  
  - Batch ingestion of CSV, JSON, and log files from raw data sources.  
  - Metadata tracking for ingestion time, source, and validation.
 
- **Data Transformation**
  - Cleansing and standardization of campaign and ad platform data.
  - Schema alignment to create a unified marketing performance dataset.
 
- **Data Loading**

- **Orchestration**  
  - **Airflow DAGs** manage scheduling, retries, and monitoring. 

- **Scalability**  
  - Local development with PostgresQL.  
    
---
## Architecture
Raw Data Sources → Ingestion (Python) → Transformation (dbt / Databricks) → Snowflake → BI & Analytics

---

## Tech Stack
- **Python** – data ingestion, validation, transformation
- **Airflow** – orchestration and scheduling 
- **Snowflake** – cloud data warehouse
- **Databricks** – large-scale processing and transformation  
- **dbt** – SQL-based transformations in Snowflake
- **PostgreSQL** – lightweight local testing
- **GitHub Actions** – CI/CD  

---

## Project Structure
cloud-marketing-pipeline/ \
│── dags/ # Airflow DAGs \
│── src/ # Python ETL scripts \
│── raw_data/ # Ignored - raw input files \
│── processed_data/ # Ignored - processed outputs \
│── sample_datasets/ # Versioned test data \
│── config/ # Pipeline configuration (YAML/INI) \
│── tests/ # Unit tests (pytest) \
│── README.md # Project documentation \
│── requirements.txt # Python dependencies
