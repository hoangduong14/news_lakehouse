News Lakehouse Platform

A fully end-to-end data pipeline for collecting, processing, and analyzing Vietnamese news articles.
This project implements a modern Lakehouse architecture using PySpark, Iceberg, GCS, BigQuery, orchestrated by Airflow, monitored via Prometheus + Grafana, and visualized through Looker dashboards.


1. Project Overview

This platform automatically crawls 20,000+ news articles from VnExpress using multi-threaded Selenium, processes the data through Bronze → Silver → Gold ETL layers, stores curated data in Iceberg tables on GCS, and enables analytics via BigQuery and Looker.

All workflows—crawling, validation, ETL, loading—are scheduled and orchestrated with Airflow, running daily.

The entire system leverages a Lakehouse approach, allowing:

Unified batch processing

Schema evolution

ACID transactions

Time-travel queries

Optimized analytics consumption


2. Architecture
<img width="2" height="1" alt="image" src="https://github.com/user-attachments/assets/9e7fa4c9-623c-40ab-8e3b-f613ca5f4681" />



3. Tech Stack

Languages & Processing

Python (Selenium, multithreading, logging)

PySpark (ETL, transformations, optimization)

Orchestration

Apache Airflow (DAGs, scheduling, retry, automation)

Lakehouse Storage

Google Cloud Storage (raw + curated zones)

Apache Iceberg (ACID table format, snapshots, schema evolution)

Analytics

BigQuery (query engine)

Looker / Looker Studio (dashboards)

Monitoring

Prometheus (metrics collection)

Grafana (system dashboards)

Others

Docker

Linux / Bash
