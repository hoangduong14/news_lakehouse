# News Lakehouse Platform

A fully end-to-end data pipeline for collecting, processing, and analyzing Vietnamese news articles.
This project implements a modern Lakehouse architecture using PySpark, Iceberg, GCS, BigQuery, orchestrated by Airflow, monitored via Prometheus + Grafana, and visualized through Looker dashboards.


## 1. Project Overview

This platform automatically crawls 20,000+ news articles from VnExpress using multi-threaded Selenium, processes the data through Bronze → Silver → Gold ETL layers, stores curated data in Iceberg tables on GCS, and enables analytics via BigQuery and Looker.

All workflows—crawling, validation, ETL, loading—are scheduled and orchestrated with Airflow, running daily.

The entire system leverages a Lakehouse approach, allowing:

- Unified batch processing

- Schema evolution

- ACID transactions

- Time-travel queries

- Optimized analytics consumption


## 2. Architecture

<img width="503" height="199" alt="image" src="https://github.com/user-attachments/assets/04361db8-7bd2-4c1c-8b46-fcfaebfa0f9f" />



## 3. Tech Stack

 #### Languages & Processing

- Python (Selenium, multithreading, logging)

- PySpark (ETL, transformations, optimization)

#### Orchestration

- Apache Airflow (DAGs, scheduling, retry, automation)

#### Lakehouse Storage

- Google Cloud Storage (bronze, silver, gold zones)

- Apache Iceberg (ACID table format, snapshots, schema evolution)

#### Analytics

- BigQuery (query engine)

- Looker / Looker Studio (dashboards)

#### Monitoring

- Prometheus (metrics collection)

- Grafana (system dashboards)

### 4. Data Ingestion
Input: In-depth selection, time required for scraping
Output: Raw JSON → stored in GCS Bronze layer
Key features:
- Retry logic
- Error handling
- Crawl by date range
- Crawl by article category

### 5. Orchestration — Airflow

Workflows run daily:
- Crawl news
- Validate raw data
- Bronze → Silver PySpark job
- Silver → Gold transformation
- Load curated data into Iceberg
- Trigger downstream tasks (BigQuery refresh, Looker caching)

### 6. ETL — PySpark (Bronze → Silver → Gold)
Bronze Layer
- Stores raw crawled JSON
- Basic schema validation

Silver Layer
- Cleansed data
- Normalized schema
- Removes duplicates
- Extract structured fields (author, tags, published_at)

Gold Layer
- Modeled into Star Schema:
5 Fact tables
7 Dimension tables
- Optimized for BI consumption

### 7. Lakehouse Storage — GCS + Iceberg

Data is stored as Apache Iceberg tables for:

- ACID transactions
- Time travel
- Snapshot isolation
- Schema evolution
- Partition pruning

### 8. Analytics — BigQuery & Looker

Final curated data in Iceberg is exposed to BigQuery for:
- Exploratory analysis
- Metric computation
- Dashboard modeling

Looker dashboards include:
- Articles Overview
- Author performance
- Analytics keyword and topic
- Community interaction analysis


### 9. Monitoring — Prometheus + Grafana

Monitored metrics:
- Pipeline runtime
- Crawl throughput
- Error rates
- Task duration
- Spark job metrics


### 10. How to Run the Project
Install dependencies
pip install -r requirements.txt

Run Selenium crawler

Submit PySpark ETL job

Start Airflow
docker-compose up airflow-webserver airflow-scheduler

Start Monitoring
docker-compose up prometheus grafana

