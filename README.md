# Avro-Encoded-Real-Time-Data-Processing
The project showcases a robust real-time data pipeline integrating Confluent Kafka, MySQL, and Avro serialization to facilitate seamless and immediate processing of e-commerce updates. The pipeline enables efficient streaming, transformation, and storage of incremental data updates for downstream analytics and business intelligence.

## Features
* Kafka Producer: Fetches incremental updates from a MySQL database and serializes data into Avro format.
* Multi-partitioned Topics: Utilizes 10 partitions to ensure optimal data distribution.
* Kafka Consumer Group: Python-based consumer group of 5 consumers deserializes Avro data, performs transformations, and writes to JSON files.
* Data Transformation: Implements logic for case conversions, price adjustments based on business rules, and efficient JSON formatting.
* Comprehensive Documentation: Includes setup guidelines, SQL queries for incremental fetch, Avro schema, and illustrated execution via screenshots.

## Technologies Used:
* Python 3.7+
* Confluent Kafka Python Client
* MySQL Database
* Apache Avro File Format

# KAFKA UI
<img width="912" alt="KAFKAUI" src="https://github.com/KRISHNASAIRAJ/Avro-Encoded-Real-Time-Data-Processing/assets/90061814/cd16e59c-9abe-4827-adb0-9a1db2e0d458">
