# AWS Batch ETL Pipeline

## Overview
End-to-end batch pipeline that ingests raw taxi trip data, 
transforms it using AWS Glue (PySpark), and loads it into 
Redshift for analytics. Athena used for ad-hoc querying.

## Architecture
S3 (raw) → AWS Glue (PySpark transform) → Redshift (warehouse) → Athena (queries)

## Tech Stack
- AWS S3
- AWS Glue (PySpark)
- Amazon Redshift
- Amazon Athena
- AWS Lake Formation

## Key Features
- PySpark transformations for data cleaning and enrichment
- Partitioned S3 storage for query optimization
- Redshift for fast analytical queries

## Project Structure
- src/ - Glue job scripts
- sql/ - Redshift queries
- architecture/ - System design diagram
- screenshots/ - Output evidence
