# Retail Data Engineering Pipeline

An end-to-end retail data engineering project that automates data processing with Apache Airflow and Apache Spark, validates data quality, stores processed results in PostgreSQL, and delivers interactive sales analytics through Streamlit.

The project uses Docker Compose to orchestrate its services in a reproducible local development environment.

## Project Overview

Retail businesses generate transaction data that must be cleaned, processed, validated, and transformed into useful information for decision-making.

This project demonstrates how to build a containerized data pipeline that automates these tasks, from raw retail sales data to processed records and interactive analytics.

### Key Features

* **Workflow orchestration:** Apache Airflow manages and schedules pipeline tasks.
* **Data processing:** Apache Spark processes retail sales data.
* **Data storage:** PostgreSQL stores pipeline data and processed results.
* **Data quality validation:** Pipeline tasks check data availability and validate processed records.
* **Interactive analytics:** Streamlit provides a dashboard for exploring retail sales information.
* **Containerization:** Docker Compose runs the project's services together.
* **Reproducibility:** Configuration templates and containerized services make the project easier to set up in another development environment.

## Architecture

```text
             Raw Retail Sales CSV
                      |
                      v
              Apache Airflow
              Workflow Orchestration
                      |
                      v
               Apache Spark
              Data Processing
                      |
                      v
                PostgreSQL
              Processed Data
                      |
                      v
              Streamlit Dashboard
              Sales Analytics
```

Docker Compose manages the supporting services and their configuration.

## Technology Stack

| Technology     | Purpose                               |
| -------------- | ------------------------------------- |
| Python         | Pipeline and application development  |
| Apache Airflow | Workflow orchestration                |
| Apache Spark   | Distributed data processing framework |
| PostgreSQL     | Relational database and data storage  |
| Streamlit      | Interactive data dashboard            |
| Docker         | Containerization                      |
| Docker Compose | Multi-service orchestration           |
| SQL            | Data validation and querying          |
| Git and GitHub | Version control and project hosting   |

## Project Structure

```text
03_retail_data_pipeline/
├── dags/
│   └── retail_pipeline.py
├── dashboard/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── data/
│   └── raw/
│       └── retail_sales.csv
├── spark/
│   ├── process_retail.py
│   └── postgresql-42.7.5.jar
├── .env.example
├── .gitignore
├── docker-compose.yaml
└── README.md
```

**Directory descriptions**

* `dags/`: Contains the Airflow DAG and task definitions.
* `dashboard/`: Contains the Streamlit dashboard and its container configuration.
* `data/raw/`: Contains the sample retail sales dataset.
* `spark/`: Contains the Spark processing script and PostgreSQL JDBC driver.
* `.env.example`: Provides a template for local environment configuration.
* `docker-compose.yaml`: Defines the services used by the project.
* `.gitignore`: Excludes local configuration, generated files, and other files that should not be committed.

## Pipeline Workflow

The Airflow DAG is named `retail_sales_pipeline`.

It consists of three tasks:

### 1. `check_raw_data`

Checks that the raw retail sales data is available before processing begins.

### 2. `process_retail_data`

Runs the Spark processing script to process the retail dataset and write the processed data to PostgreSQL.

### 3. `validate_processed_data`

Validates the processed data after the Spark processing task completes.

The tasks execute in dependency order, ensuring that data checks and processing happen before final validation.

## Services and Local Access

The project exposes the following local interfaces when the relevant services are running:

| Service                | URL                   | Purpose                               |
| ---------------------- | --------------------- | ------------------------------------- |
| Apache Airflow         | http://localhost:8080 | Monitor and manage pipeline workflows |
| Apache Spark Master UI | http://localhost:8081 | Inspect Spark application activity    |
| Streamlit Dashboard    | http://localhost:8501 | Explore retail sales analytics        |

These addresses are intended for local development and are not public deployment URLs.

## Prerequisites

Before running the project, install:

* Docker Desktop
* Git
* A terminal such as PowerShell on Windows

Ensure Docker Desktop is running and has sufficient resources for the configured containers.

## Setup and Installation

### 1. Clone the repository

```bash
git clone https://github.com/mr-wes/retail-data-pipeline.git
cd retail-data-pipeline
```

### 2. Configure environment variables

Create your local `.env` file from the supplied template.

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Open `.env` and replace the placeholder values with appropriate local development settings.

Update `PROJECT_HOST_DIR` to the absolute path of your project directory on your own computer.

Ensure the PostgreSQL credentials and Airflow database credentials match the configuration expected by the Docker Compose services.

**Security note:** Never commit your `.env` file, passwords, encryption keys, or other secrets to GitHub. The repository includes `.env.example` as a configuration template.

### 3. Start the services

After reviewing the configuration, run:

```bash
docker compose up -d
```

Allow the services to initialize before accessing their interfaces.

### 4. Monitor the pipeline

Open the Airflow interface:

http://localhost:8080

Locate the `retail_sales_pipeline` DAG and inspect its task statuses and execution logs.

### 5. View the Spark interface

Open:

http://localhost:8081

Use the Spark interface to inspect application activity when a processing job is running or its status is available.

### 6. Open the dashboard

Open:

http://localhost:8501

Explore the retail sales visualizations provided by the Streamlit application.

**Note:** The commands above describe a fresh local setup. If the project is already running, review its current state before restarting or reinitializing services. Do not delete Docker volumes when you need to preserve database data.

## Data Quality and Validation

Data quality is an important part of a reliable data engineering workflow.

This project includes pipeline tasks for checking raw data availability and validating processed records.

During local testing, the pipeline completed successfully, and the processed dataset was checked for basic consistency.

The recorded test results included:

* 20 retail transactions
* Total revenue of 7,560.00 in the dataset's currency units
* 59 units sold
* 20 unique transaction identifiers
* No duplicate transaction identifiers detected in the validation query

These figures describe the sample dataset used during testing and are not live business metrics.

## Engineering Considerations

The project demonstrates several practical data engineering principles:

* Separating orchestration from data processing.
* Automating repeatable data workflows.
* Validating data after processing.
* Using a relational database for structured data storage.
* Containerizing services to simplify local development.
* Managing configuration through environment variables.
* Keeping local secrets and generated artifacts out of version control.

## Future Improvements

Potential enhancements include:

* Adding automated unit and integration tests.
* Expanding validation to cover missing values, invalid records, and business rules.
* Adding structured logging and improved failure notifications.
* Implementing incremental processing for larger datasets.
* Introducing data quality reporting and pipeline monitoring.
* Adding continuous integration checks through GitHub Actions.
* Deploying the system to a cloud environment.
* Improving dashboard filtering and business performance metrics.



## Author

**Joshua Wesley**

GitHub: [@mr-wes](https://github.com/mr-wes)

