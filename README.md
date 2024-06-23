# Airflow ETL for Weather Data

## Project Overview

This project implements an ETL (Extract, Transform, Load) pipeline using Apache Airflow to fetch weather data from the OpenWeatherMap API and load it into a SQL Server database. The pipeline is designed to run daily, extracting the latest weather data for a specified city, transforming the data into a structured format, and loading it into the database for further analysis.

## Features

- **Data Extraction**: Fetch weather data from the OpenWeatherMap API.
- **Data Transformation**: Process and structure the extracted data.
- **Data Loading**: Insert the transformed data into a SQL Server database.
- **Automation**: Schedule and manage the ETL workflow using Apache Airflow.

## Prerequisites

- Docker
- Docker Compose
- OpenWeatherMap API key
- Microsoft SQL Server
- Python 3.x
- Git
