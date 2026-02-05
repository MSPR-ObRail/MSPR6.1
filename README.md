# ObRail Europe - ETL Pipeline

A data pipeline for extracting, transforming, and analyzing European railway route data with CO2 emissions calculation.

## Overview

This project processes GTFS (General Transit Feed Specification) data from multiple European rail operators across day and night routes to calculate carbon emissions and generate insights.

## Features

- **Multi-country Data Support**: Denmark, France, Germany, Switzerland, and international services
- **Day & Night Routes**: Separate pipelines for daytime and night train services
- **CO2 Calculation**: Automatic emissions calculation for all routes
- **Data Cleaning**: Automated data validation and transformation
- **ETL Pipeline**: Modular extract, transform, load processes

## Quick Start

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Run the pipeline:
   ```bash
   python main.py
   ```

## Project Structure

- **scripts/**: Data processing modules (calculate_co2.py, clean_routes.py, etc.)
- **data/**: Raw and processed data files
- **config/**: Configuration settings
- **logs/**: Pipeline execution logs

## Data Sources

- GTFS feeds from official European transport authorities
- CO2 emission factors from EEA (European Environment Agency)
- Includes Eurostar, SNCF, ÖBB, and regional operators

## Technologies

- Python 3.x
- PySpark for large-scale data processing
- Pandas for data manipulation
- FastAPI for API endpoints
