# Project to practice Python / Kafka / Flink

## Architecture

![Flink-weather-streaming](https://github.com/skalskibukowa/Project-Kafka-Flink/assets/29678557/2b47daa2-4152-4bf4-9db4-464bf39b2479)

## Overview

The project simulates continuously reads weather data from a Kafka topic, calculates the average temperature for each city every minute, and stores the results in a PostgreSQL database.

Here's a detailed breakdown of the project:

**1. Kafka Producer:**


**gen_data()** function generates random weather data every 10 seconds and sends it to the weather topic in Kafka.

**2. Flink Processor:**

**Main.java** is the main entry point for the Flink application.
**WatermarkStrategy** assigns watermarks to events, ensuring that late-arriving events are not processed.

**WeatherDeserializationSchema** is a custom deserialization schema that reads JSON-formatted weather data from Kafka.

**Main() function** creates a Flink streaming environment and reads weather data from the weather topic using KafkaSource.

**AverageAggregator** is an aggregation function that calculates the average temperature for each city every 60 seconds.

**cityAndValueStream** is a DataStream containing the city and average temperature for each city.

**JdbcSink** stores the calculated average temperatures in the weather table in PostgreSQL.

**3. Data Structures:**

**Weather class** represents a weather data point with city and temperature attributes.
**MyAverage class** is a helper class used by the AverageAggregator function to store intermediate aggregation results.

Overall, the project utilizes Kafka for real-time data ingestion, Flink for data processing and aggregation, and PostgreSQL for data storage.

## Instructions for Building and Running a Flink Application with Kafka and PostgreSQL

This document provides comprehensive instructions for building and running a Flink application that ingests weather data from Kafka, processes it, and stores the results in a PostgreSQL database.

### Prerequisites:

Docker and Docker Compose installed
Familiarity with Flink, Kafka, and PostgreSQL
Steps:

### Project Structure:

a. Create a directory for each component:
* postgres: For the PostgreSQL database
* kafka-producer: For the Kafka producer
* flink-processor: For the Flink processor

### PostgreSQL Setup:

a. Create a Dockerfile for PostgreSQL:
* Expose port 5432 for database access
* Copy the docker-compose.yml file

b. Create a docker-compose.yml file for the PostgreSQL container:
* Define the PostgreSQL image
* Map host port 5432 to container port 5432
* Mount a data directory for persistence

### Kafka Producer Setup:

a. Create a directory for Kafka Producer scripts:
* scripts: For PowerShell and Python scripts

b. Create a PowerShell script (.ps1) to generate weather data:
* Generate random weather data
* Use Kafka Producer API to send weather data to Kafka topic

c. Create a requirements.txt file to list Python dependencies:
* Kafka Python library for producing data to Kafka

d. Create a Python script (.py) to call the PowerShell script:
* Execute the PowerShell script to generate weather data

e. Create a Dockerfile for the Kafka Producer:
* Install Python 3.10
* Install Kafka Python library
* Copy scripts and Python script
* Run the Python script

### Flink Processor Setup:

a. Create a directory for Flink application resources:
* src/main/java: For Java source code
* resources: For resource files (log4j2.properties)

b. Create a log4j2.properties file for logging configuration:
* Set logging level to INFO
* Configure console logging pattern

c. Create a Dockerfile for the Flink Processor:
* Use Flink:1.14.4 image
* Copy Flink application JAR file
* Copy log4j2.properties file
* Specify Flink command to execute application JAR

d. Create a main class (Main.java) to handle weather data processing and ingestion into PostgreSQL:
* Create a Flink streaming job
* Read data from Kafka topic
* Process and transform weather data
* Write processed data to PostgreSQL database

### Build and Run:

a. Build the Kafka Producer Docker image:
* cd kafka-producer && docker build -t kafka-producer .

b. Build the Flink Processor Docker image:
* cd flink-processor && mvn clean package
* docker build -t flink-processor .

c. Start the Docker containers:
* docker-compose up -d

d. Verify PostgreSQL connection:
* docker exec -it postgres-flink psql -U postgres -d postgres
* \dt
* select * from weather
