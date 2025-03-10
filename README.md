# axpo_fed_team_training

This repository contains code and resources for training and working with Databricks, Delta Live Tables (DLT), and data generation using PySpark and dbldatagen. Below is a summary of the contents of this repository:

## Files and Notebooks

### 1. `0_test_data_generation.py`
This script is responsible for generating test data. It creates schemas, volumes, and generates plans, customers, and events data using `dbldatagen`. **Start by running this script to set up the necessary test data.**

### 2. `DLT Pipeline 1.py`
This notebook introduces Delta Live Tables (DLT) and demonstrates how to create streaming tables, materialized views, and views for data processing.

### 3. `DLT Code 2.sql`
This SQL notebook works in conjunction with `DLT Pipeline 1.py` and `SCD's.py` to create a complete pipeline for data processing.

### 4. `Example constructs.sql`
This notebook contains various SQL constructs and examples, including creating streaming tables, materialized views, and using higher-order functions.

### 5. `Generate views.py`
This script generates views for bronze, silver, and gold tables based on the existing tables in the catalog.

### 6. `pandas to pyspark based pandas.py`
This script demonstrates how to convert a pandas DataFrame to a PySpark DataFrame using `pyspark.pandas`.

### 7. `SCD's.py`
This notebook demonstrates how to create Slowly Changing Dimensions (SCD) using Delta Live Tables (DLT).

### 8. `test_notebook.py`
This notebook contains various test cases and examples created during a workshop.

### 9. `test_notebook 2.sql`
This SQL notebook collects SQL 1 statement pipelines and demonstrates how to integrate them into a larger end-to-end pipeline.

## Getting Started

1. **Run `0_test_data_generation.py`**: This script sets up the necessary test data for the other notebooks and scripts.
2. Explore the other notebooks and scripts to understand how to work with Databricks, Delta Live Tables, and data generation using PySpark.

## License

This project is licensed under the MIT License.