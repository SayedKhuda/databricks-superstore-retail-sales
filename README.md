# Databricks Superstore Retail Sales Project

## Project Overview

This project demonstrates an end-to-end retail sales data engineering and analytics solution built in Databricks.

The Superstore sales dataset was ingested into Databricks, cleaned and validated using PySpark, stored as Delta tables, transformed into business-level Gold tables, and finally visualised using a Databricks Dashboard.

## Technologies Used

- Databricks
- Apache Spark
- PySpark
- Delta Lake
- Unity Catalog
- SQL
- Databricks Dashboards

## Data Architecture

The project follows a Medallion-style data processing approach:

**Raw Data → Data Cleaning & Validation → Silver Layer → Gold Layer → Dashboard**

### Raw Data
The Superstore CSV dataset was uploaded to a Unity Catalog Volume and read using PySpark.

### Silver Layer
The raw data was cleaned and validated by:

- Standardising column names
- Converting date columns
- Converting numeric data types
- Checking duplicate records
- Checking null values
- Identifying invalid records
- Separating rejected records from valid records

The cleaned data was stored as a Delta table.

### Gold Layer
Gold tables were created from the Silver data to answer stakeholder business requirements.

## Business Requirements

The analysis provides:

1. Total customers
2. Total orders
3. Total sales
4. Total profit
5. Sales by country
6. Profit by region and country
7. Sales by category
8. Top 10 sub-categories by sales
9. Top products by ordered quantity
10. Top customers by sales and city

## Dashboard

An interactive Databricks Dashboard was created to present the final business insights.

The dashboard includes:

- KPI cards for Customers, Orders, Sales and Profit
- Profit by Region
- Sales by Category
- Top 10 Sub-Categories by Sales
- Sales by Country
- Top Products by Ordered Quantity
- Top Customers by Sales and City

## Key Results

- **Total Customers:** 793
- **Total Orders:** 5,008
- **Total Sales:** approximately 2.30M
- **Total Profit:** approximately 286.39K

## Data Quality

The raw dataset contained **9,994 rows**.

Data quality checks included duplicate detection, null-value checks, schema validation, safe data-type conversion and rejected-record handling.

## Project Structure

```text
databricks-superstore-retail-sales/
│
├── README.md
├── notebooks/
├── data/
├── sql/
├── screenshots/
└── docs/
