# Music Streaming Analytics Pipeline

A comprehensive AWS data engineering solution for music streaming analytics using Redshift Serverless and AWS Glue for real-time KPI computation and business intelligence.

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Data Pipeline](#data-pipeline)
- [Setup Instructions](#setup-instructions)
  - [1. S3 Data Lake Structure](#1-s3-data-lake-structure)
  - [2. Redshift Serverless Environment](#2-redshift-serverless-environment)
  - [3. AWS Glue Connection](#3-aws-glue-connection)
  - [4. ETL Jobs Deployment](#4-etl-jobs-deployment)
- [KPI Metrics](#kpi-metrics)
- [Security & Best Practices](#security--best-practices)
- [Cost Optimization](#cost-optimization)
- [Limitations](#limitations)
- [Contributing](#contributing)

## Overview

This project implements a complete music streaming analytics pipeline that processes user listening data, song metadata, and user information to generate business KPIs. The solution leverages AWS serverless technologies for automatic scaling and cost optimization.

**Key Features:**
- **Real-time ETL processing** with 3-stage Glue jobs
- **Genre-based analytics** with popularity metrics
- **Hourly listening patterns** and user behavior analysis
- **UPSERT operations** to Redshift with staging tables
- **Data quality validation** at each transformation step
- **Parquet optimization** for storage and query performance

## Prerequisites

Before you begin, ensure you have:

-  **AWS Account** with free tier access
-  **AWS CLI** configured with Access Key ID and Secret Access Key
-  Basic knowledge of **AWS Services** (S3, IAM, VPC)
-  **SQL** understanding for data querying
-  Multiple AWS CLI profiles (optional, for different IAM users)

## Architecture

```
Raw Data (S3)                    Curated Data (S3)                Analytics (S3 + Redshift)
├── streams/                     ├── processed-metadata/          ├── kpi_output/
│   └── *.csv                    │   ├── songs/                   │   └── *.parquet
├── metadata/                    │   └── users/                   └── Redshift Tables
│   ├── songs.csv                └── mss-transformed/                 ├── kpi_table
│   └── users.csv                    └── *.parquet                   └── kpi_staging
        ↓                                ↓                               ↓
   [Glue Job 1]              →     [Glue Job 2]           →        [Glue Job 3]
Metadata Processing              Data Integration               KPI Computation
```

**Data Flow:**
1. **Job 1**: Process and validate metadata (songs + users)
2. **Job 2**: Join streaming data with metadata 
3. **Job 3**: Calculate KPIs and load to Redshift

## Data Pipeline

### Job 1: Metadata Processing (`metadata_processing.py`)
- **Source**: Raw CSV files (`songs.csv`, `users.csv`)
- **Transformations**: Schema enforcement, data type conversion
- **Output**: Processed metadata in Parquet format
- **Tables Created**: `processed_songs_table`, `processed_users_table`

### Job 2: Data Integration (`data_integration.py`)
- **Sources**: Streaming data (CSV) + processed metadata (Glue Catalog)
- **Transformations**: Left joins to enrich streaming data
- **Output**: Unified dataset with all attributes
- **Table Created**: `dim-music-streaming`

### Job 3: KPI Computation (`kpi_computation.py`)
- **Source**: Transformed streaming data (Parquet)
- **Transformations**: Aggregations, window functions, KPI calculations
- **Output**: Analytics-ready KPIs in Redshift + S3 audit trail
- **Tables**: `kpi_table` (final), `kpi_staging` (temporary)

## Setup Instructions

### 1. S3 Data Lake Structure

Create the following S3 bucket structure:
```
lab1-rawdata/
├── streams/              # Raw streaming data CSV files
└── metadata/
    ├── songs.csv         # Track metadata (track_id, artists, track_name, popularity, duration_ms, track_genre)
    └── users.csv         # User data (user_id, created_at)

lab1-curateddata/
├── processed-metadata/
│   ├── songs/            # Processed song metadata (Parquet)
│   └── users/            # Processed user metadata (Parquet)  
└── mss-transformed/      # Integrated streaming + metadata (Parquet)

lab1-analytics/
└── kpi_output/           # Final KPI results (Parquet, partitioned by kpi_type)

lab1-temp/
└── redshift-tmp/         # Temporary files for Redshift operations
```

### 2. Redshift Serverless Environment

#### Create Workgroup
1. Navigate to **Amazon Redshift** dashboard
2. Click **"Create workgroup"**
3. Configure workgroup settings:
   ```
   Workgroup name: sandbox-workspace
   VPC: Select your VPC
   Security Group: Default or custom
   Subnets: Select all for full connectivity
   Enhanced VPC routing: Optional
   ```

#### Create Namespace and Database
1. Configure namespace:
   ```
   Namespace name: sandbox
   Database name: dev (or custom)
   Admin credentials: Choose password option
   ```
2. **Associate IAM Role** for S3 access
3. Review **Encryption settings** (AWS-owned KMS key by default)
4. **Create environment** (takes a few minutes)

#### Connection Methods
- **JDBC/ODBC endpoints**: Available on workgroup dashboard
- **Query Editor V2**: Browser-based access from Redshift dashboard
- **Programmatic access**: Using database credentials

### 3. AWS Glue Connection

#### Configure IAM Role
Create a new IAM role with these permissions:
```
- RedshiftFullAccess
- S3FullAccess
- AWSGlueConsoleFullAccess
- AWSGlueServiceRole
- AWSKeyManagementServicePowerUser
- SecretsManagerReadWrite
```

Set **AWS Glue** as trusted entity.

#### Create VPC Endpoints
Create Interface/Gateway endpoints for:
- **S3** (Gateway endpoint)
- **Redshift** (Interface endpoint)
- **Secrets Manager** (Interface endpoint)
- **KMS** (Interface endpoint)
- **STS** (Interface endpoint)

**Important:** Associate all subnets connected to your Redshift environment.

#### Configure Security Group
Add inbound rule to Redshift Security Group:
```
Type: All TCP
Source: Security Group ID (self-referencing)
```

#### Create Glue Connection
1. Navigate to **AWS Glue Console** → **Connections**
2. Click **"Create connection"** → Select **Redshift**
3. Configure connection:
   ```
   Redshift Environment: Select your serverless cluster
   Database: dev
   Username: admin
   Password: Your chosen password
   IAM Role: Previously created role
   ```
4. **Test connection** to verify setup

### 4. ETL Jobs Deployment

#### Create Glue Database
```sql
CREATE DATABASE IF NOT EXISTS mss_db;
```

#### Create Redshift Tables
```sql
-- Main KPI table
CREATE TABLE IF NOT EXISTS public.kpi_table (
    kpi_type VARCHAR(20),                 -- 'genre' or 'hourly'
    group_by VARCHAR(100),                -- genre name or hour (0-23)
    genre_listen_count BIGINT,            -- listen count or unique listeners
    genre_avg_duration DOUBLE PRECISION, -- avg duration or diversity index
    genre_popularity_index DOUBLE PRECISION, -- popularity score (genre only)
    top_track VARCHAR(255),               -- most popular track name
    top_artist VARCHAR(255)               -- most popular artist
);

-- Staging table (created automatically by Job 3)
CREATE TABLE IF NOT EXISTS public.kpi_staging (
    kpi_type VARCHAR(20),
    group_by VARCHAR(100),
    genre_listen_count BIGINT,
    genre_avg_duration DOUBLE PRECISION,
    genre_popularity_index DOUBLE PRECISION,
    top_track VARCHAR(255),
    top_artist VARCHAR(255)
);
```

#### Deploy Glue Jobs in Sequence

**Job 1: Metadata Processing**
- **Purpose**: Clean and validate raw metadata
- **Input**: `s3://lab1-rawdata/metadata/`
- **Output**: `s3://lab1-curateddata/processed-metadata/`
- **Key Features**:
  - Schema enforcement (track_genre → genre)
  - Data type conversion (strings → appropriate types)
  - Data quality validation
  - Parquet output with Snappy compression

**Job 2: Data Integration**  
- **Purpose**: Join streaming data with metadata
- **Input**: Raw streams + processed metadata from Glue Catalog
- **Output**: `s3://lab1-curateddata/mss-transformed/`
- **Key Features**:
  - Left joins to preserve all streaming records
  - Column renaming for clarity
  - Unified schema creation
  - Catalog integration

**Job 3: KPI Computation**
- **Purpose**: Calculate business metrics and load to Redshift
- **Input**: `s3://lab1-curateddata/mss-transformed/`
- **Output**: Redshift `kpi_table` + S3 audit trail
- **Key Features**:
  - Genre-level KPIs (listen count, avg duration, popularity)
  - Hourly analysis (unique listeners, track diversity)
  - Top track/artist identification using window functions
  - UPSERT operations with staging table pattern

## KPI Metrics

### Genre-Level Analytics
- **Listen Count**: Total plays per genre
- **Average Duration**: Mean track length in milliseconds
- **Popularity Index**: Average popularity score
- **Top Track**: Most popular song by genre
- **Top Artist**: Most popular artist by genre

### Hourly Analytics  
- **Unique Listeners**: Distinct users per hour
- **Track Diversity Index**: Ratio of unique tracks to total plays
- **Top Artist**: Most played artist per hour

### Sample KPI Queries
```sql
-- Genre performance ranking
SELECT 
    group_by as genre,
    genre_listen_count,
    genre_popularity_index,
    top_artist
FROM public.kpi_table 
WHERE kpi_type = 'genre'
ORDER BY genre_listen_count DESC;

-- Peak listening hours
SELECT 
    group_by as hour,
    genre_listen_count as unique_listeners,
    genre_avg_duration as diversity_index
FROM public.kpi_table 
WHERE kpi_type = 'hourly'
ORDER BY unique_listeners DESC;
```

## Security & Best Practices

### Security Measures
-  **IAM Policies** for access control
-  **VPC-based** Redshift access
-  **Encryption at rest** using KMS/HSM
-  **Audit logging** enabled
-  **VPC endpoints** for private communication

### Best Practices
- Use **least privilege** IAM policies
- Enable **Enhanced VPC routing**
- Implement **network segmentation**
- Regular **security audits**

## Cost Optimization

### Storage & Performance
- Use **Parquet format** for optimized storage
- Implement **data partitioning** for efficient queries
- Apply **S3 lifecycle policies** for older data

### Serverless Benefits
- **Pay-per-use** pricing model
- **Automatic scaling** based on workload
- **No infrastructure management**
- **RPU-hour** billing (Redshift Processing Units)

## Limitations

### Redshift Serverless Current Limitations
- Parameter Groups not supported
- Workload Management (WLM) unavailable
- AWS Partner integration limited
- No maintenance windows/version tracks
- Public endpoints not supported (VPC access only)


## OLAP

![alt text](<misc/Screenshot from 2025-06-14 14-08-52.png>)

![alt text](<misc/Screenshot from 2025-06-14 14-10-50.png>)

![alt text](<misc/Screenshot from 2025-06-14 14-11-18.png>)