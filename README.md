# Apache Paimon Streaming Data Lakehouse Platform

This project implements the complete architecture, infrastructure blueprint, and step-by-step operational guide for an enterprise-grade, high-frequency Streaming Data Lakehouse platform powered by **Apache Paimon (0.8+/1.0+)**.

Apache Paimon is a unified lakehouse format designed to process streaming mutations at high velocity (UPSERT/DELETE in real-time) and provide low-latency queries across both Batch and Streaming engines. Leveraging its Log-Structured Merge-tree (LSM-tree) architecture, Paimon enables sub-minute ingestion latencies and highly optimized read/write performance over S3-compatible Object Storage.

---

## 1. System Architecture & Apache Paimon Core Concepts

### 1.1 Architectural Flow Diagram (ASCII)

```text
                                  +---------------------------------------+
                                  |      Streaming Data Source (CDC)      |
                                  +---------------------------------------+
                                                      |
                                                      v
                                        +---------------------------+
                                        |    Apache Kafka (Topic)   |
                                        +---------------------------+
                                                      |
                                                      | (Continuous Consume)
                                                      v
                                    +-----------------------------------+
                                    | Apache Flink (Streaming Ingestion)|
                                    +-----------------------------------+
                                                      |
                                                      | (Paimon Catalog & S3 Writer)
                                                      v
+---------------------------------------------------------------------------------------------------------+
|                                    MINIO / S3 OBJECT STORAGE LAYER                                      |
|                                                                                                         |
|  +---------------------------------------------------------------------------------------------------+  |
|  |                                  APACHE PAIMON TABLE STRUCTURE                                    |  |
|  |                                                                                                   |  |
|  |  [Paimon FileSystem Catalog Metadata]                                                             |  |
|  |                                                                                                   |  |
|  |  +---------------------------------------------------------------------------------------------+  |  |
|  |  |  LSM-Tree File Layout (Partition: dt=YYYY-MM-DD / Bucket: bucket-0, bucket-1, etc.)           |  |  |
|  |  |                                                                                             |  |  |
|  |  |  +-----------------------------+   +-----------------------------+   +--------------------+  |  |  |
|  |  |  | MemTable (In-Memory Buffer) | ->| L0 SST (Immutable Run)      | ->| L1 SST (Sorted Run)|  |  |  |
|  |  |  | (Incoming Upserts / Deletes)|   | (Uncompacted, Overlapping)  |   | (Fully Compacted)  |  |  |  |
|  |  |  +-----------------------------+   +-----------------------------+   +--------------------+  |  |  |
|  |  |                                                                                             |  |  |
|  |  |  +---------------------------------------------------------------------------------------+  |  |  |
|  |  |  | Changelog Producer (lookup / full-compaction)                                         |  |  |  |
|  |  |  | Generates precise delta change files (+I, -U, +U, -D) for downstream consumer engines |  |  |  |
|  |  |  +---------------------------------------------------------------------------------------+  |  |  |
|  |  +---------------------------------------------------------------------------------------------+  |  |
|  +---------------------------------------------------------------------------------------------------+  |
+---------------------------------------------------------------------------------------------------------+
       |                                                 |                                   |
       | (Changelog Stream)                              | (Ad-hoc Batch SQL)                | (Interactive OLAP)
       v                                                 v                                   v
+-------------------------------+               +------------------+               +------------------+
|   Apache Spark Structured     |               |   Apache Spark   |               |   Trino /        |
|     Streaming Consumer        |               |   Batch SQL      |               |   StarRocks      |
+-------------------------------+               +------------------+               +------------------+
```

### 1.2 Paimon vs. Alternative Formats (Iceberg / Delta Lake)

While Apache Iceberg and Delta Lake are excellent choices for standard analytical batch operations, they face severe performance bottlenecks under high-frequency stream-writing and CDC-ingestion constraints. Apache Paimon offers unique structural advantages:

| Feature / Metric | Apache Paimon | Apache Iceberg | Delta Lake |
| :--- | :--- | :--- | :--- |
| **Primary Indexing Strategy** | **LSM-tree (Log-Structured Merge-tree)** | B-tree-like / Manifest-based positional deletes | B-tree-like / Parquet-based deletion vectors |
| **Upsert / Mutation Latency** | **Seconds (via memory buffers & sorted SSTs)** | Minutes (requires heavy Copy-on-Write / Merge-on-Read) | Minutes (requires heavy transactional locks) |
| **Real-time Deduplication** | **Native & immediate during SST compactions** | Done during query runtime (expensive CPU cycles) | Done during query runtime or manual MERGE |
| **Changelog Production** | **Built-in & streaming-native** (Lookup, Input, or Full-Compaction options) | Limited to scanning consecutive manifests (heavy memory overhead) | Limited to CDF (Change Data Feed) log audits |
| **Bucket-Level Isolation** | **Deterministic bucket keys with fast lookups** | Global sorting or partitioning | Dynamic file partitioning only |

#### Core Differentials of Apache Paimon:
1. **LSM-Tree Storage Architecture:**
   Paimon stores data files in an LSM-tree structure. High-frequency updates are appended to memory buffers (MemTables) and periodically flushed as Sorted String Tables (SSTs) in Layer 0. Overlapping runs are progressively compacted into deeper, sorted, and non-overlapping layers. This converts random mutations into fast sequential writes.
2. **Native Merge-on-Read and Deduplication:**
   Rather than applying expensive table-wide locks or scanning complex manifest pointers, Paimon executes automatic merge-deduplication during background compactions. While querying, users get consistent deduplicated state natively with minimal overhead.
3. **Optimized Streaming Changelogs:**
   Traditional formats compute changelogs (row mutations) by executing expensive diffs between snapshot states. Paimon's table engine can generate changelogs *during* the ingestion phase (via options like `changelog-producer = 'lookup'` or `'full-compaction'`), allowing downstream pipelines to read delta-changes directly with absolute accuracy.

---

## 2. Deep-Dive Tuning, Operation, and Monitoring Guide

### 2.1 Bucket Sizing & Distribution
In Apache Paimon, tables can be structured into one or multiple buckets to distribute writes and parallelize reads.
* **Fixed Bucket Mode (`bucket = 'N'`):** Excellent for streaming write scenarios. Each bucket is physically mapped to a directory containing its own LSM-tree.
* **Dynamic Bucket Mode (`bucket = '-1'`):** Automatically scales the bucket count as the table size grows. *Note: Dynamic bucket mode does not support downstream streaming consumption using certain changelog producers.*
* **Sizing Rule of Thumb:** Design each bucket's physical size to land between **200MB and 1GB** after compaction.
  * For small tables (< 10GB total): use `bucket = '2'` or `bucket = '4'`.
  * For medium tables (10GB - 500GB): compute `Number of Buckets = Total Expected Data Volume / 1GB`.
  * **Bucket Key:** Always define a highly-cardinal key as the `bucket-key` (often the primary key or a subset of it) to avoid data skew across buckets.

### 2.2 Selecting the Ideal Merge Engine
The `merge-engine` parameter specifies how row-mutations are merged under duplicate keys:
* **`deduplicate` (Default):** Retains only the latest row based on ingestion sequence or a specified row-kind column. Ideal for traditional replication (CDC mirror).
* **`partial-update`:** Allows late-arriving columns to update fields of an existing row. Null values do not overwrite existing values unless configured. Perfect for assembling wide profiles from multiple streaming streams.
* **`aggregation`:** Aggregates fields having duplicate keys using functions like `sum`, `min`, `max`, `listagg`, etc. Essential for real-time streaming rollups and pre-aggregations.
* **`first-row`:** Keeps only the first inserted row under a duplicate key, ignoring subsequent modifications. Extremely useful for recording initial attribution, audit logs, or clickstream click-starts.

### 2.3 Compaction Strategies: Tuning and Managing Small Files
Continuous streaming writes produce a substantial volume of small, uncompacted L0 SST files. Managing compaction is crucial to prevent "read amplification."
* **Auto-Compaction in Flink:**
  By default, Paimon write tasks perform auto-compaction in background threads. Set `'write-only' = 'false'` (default) to let the streaming engine manage compactions on-the-fly.
* **Dedicated Compaction Jobs:**
  For high-throughput systems, isolate ingestion from compactions by setting `'write-only' = 'true'` in the ingestion pipeline, and launch a separate Spark or Flink compaction job to keep compaction overhead out of the main write path.
* **Critical Tuning Parameters:**
  * `compaction.max.size-amplifier`: Controls compaction triggers based on size growth.
  * `compaction.min.file-num`: Number of files in a level that triggers compaction (default: `4`).
  * `num-sorted-run.max`: Limits total sorted runs to maintain query efficiency.

---

## 3. Repository Structure

This directory is organized as follows:
* `docker-compose.yml`: Local multi-container Docker deployment of Apache Flink, Apache Spark, Apache Kafka, and MinIO.
* `flink/01_catalog_and_ingestion.sql`: Flink SQL scripts to register the Paimon Catalog and build the continuous CDC ingestion pipeline.
* `spark/paimon_analytics.py`: Python PySpark script detailing analytic batch reads, streaming consumption, time-travel queries, and administrative maintenance routines.
