<h1 align="center">Real Time Data Analysis Project</h1>

Welcome to the Real Time Data Analysis Project - a robust, scalable, and real-time analytics solution designed to provide insightful dashboards and enhance business decisions. This project harnesses the power of Kafka, Spark Streaming, PostgreSQL, and Superset to process and visualize data as it arrives, giving you the ability to analyze trends and patterns instantaneously.

## Project Overview
At the heart of this project is a stream-stream join operation that combines data from two key tables: impression and click. These tables capture user interactions and engagements with digital content in real-time, providing a comprehensive view of user behavior and advertisement performance.

\
Here are some applications used in the project:
* **Apache Kafka**: 
    * Distributed System: Kafka is run as a cluster on one or more servers that can span multiple datacenters. This provides high availability and redundancy.
    * Topic-based Publish-Subscribe Model: Kafka maintains feeds of messages in categories called topics. In this project, separate topics for impression and click events allow consumers to subscribe to the relevant data streams.
    * Scalability: It can scale easily without incurring downtime, handling millions of messages per second.
    * Durability and Reliability: Kafka replicates data and can support failure of nodes within the cluster.
    * Performance: High throughput for both publishing and subscribing. This means users can get real-time insights as data is produced and consumed continuously.
* **Spark Streaming**:
    * Handling Event-time and Watermarks: With the concept of event-time, Spark Streaming provides capabilities to handle the ordering of events as they are ingested from Kafka, which may not be in the exact order of their occurrence. Watermarks are used to specify how late the system is supposed to wait for the late events.
    * Fault Tolerance: Spark Streaming's checkpointing feature ensures stateful and fault-tolerant processing, automatically recovering from failures.
    * Advanced Analytics: Beyond just joining streams, Spark Streaming can perform a variety of complex operations such as windowing functions, aggregation, and stateful computations.
    * Integration with Kafka: Spark Streaming integrates well with Kafka to consume stream data. It can either pull data from Kafka at a high level using offsets or track the processed data using low-level API to provide exactly-once semantics.
    * Micro-batch Processing: Unlike traditional streaming systems that process one record at a time, Spark Streaming processes data in micro-batches, which can lead to better throughput and more efficient resource utilization.

* **PostgreSQL**:
    * Robust relational database management system.
    * Stores and manages the processed and joined data.
    * Supports complex queries needed for deep analytical insights.

* **Apache Superset**:
    * Modern data exploration and visualization platform.
    * Connects to PostgreSQL for data retrieval.
    * Enables the creation of interactive dashboards for real-time data analytics.