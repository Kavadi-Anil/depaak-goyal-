Delta Tables in Spark
======================

Reading a DataFrame:
---------------------
df = spark.read.csv("location", header=True, inferSchema=True, sep=",")

Writing as a Delta Table:
--------------------------
df.write.mode("append").format("delta").saveAsTable("bronze.orders")

Inspecting the Table:
----------------------
%sql
DESCRIBE EXTENDED bronze.orders
→ Shows table details including: provider = delta

Delta Storage:
--------------
- Data is stored in **Parquet format**
- Operation logs (e.g., UPDATE, DELETE) are stored in **_delta_log** as JSON and CRC files
- For every operation, a new JSON + CRC file is created

Example:
UPDATE bronze.orders SET country = 'India' WHERE country = 'Libya'
→ Creates a new Parquet file and a new log entry in _delta_log

Time Travel in Delta
=====================

Delta allows querying older versions of data using:

By Version:
SELECT * FROM bronze.orders VERSION AS OF 2

By Timestamp:
SELECT * FROM bronze.orders TIMESTAMP AS OF '2024-08-01'

📌 Retention Period:
- Time travel is only supported **up to 30 days** (default)
- Data/logs older than that may be purged

Delta Maintenance
==================

Problem:
- Over time, many small log and data files (e.g., 1,000,000 logs) can hurt performance

🔧 Solution: Vacuum
Deletes logs older than the retention period:
VACUUM bronze.orders

🔁 Restore:
Restores data to a previous version:
RESTORE TABLE bronze.orders TO VERSION AS OF 1

Delta Table Optimization – Compaction
=====================================

If records are ingested every 5 mins:
- Many small Parquet + log files are created
- Performance is reduced due to file overhead

✅ Solution: Compaction
- Merges many small files into large files (e.g., ~1GB each)
- Improves read performance

Z-Ordering for Query Optimization
===================================

Use Z-Ordering to optimize specific column-based queries.

Example:
SELECT * FROM bronze.orders WHERE country = 'India'

→ Apply Z-Ordering:
OPTIMIZE bronze.orders ZORDER BY (country)

Benefits:
- Stores similar values together in same partition
- Improves filtering by minimizing file scans

⚠️ Z-Ordering is not effective for:
- Queries like: SELECT * FROM table
- Range queries like: WHERE id > 100

📌 Best for TB-scale data. For GB-scale, Delta is already fast.

Cluster Configuration Strategy
===============================

🧠 Formula:
Required RAM = Data Size / (5 to 10)

Example:
Data = 100 GB → RAM needed ≈ 10 GB to 20 GB

If cluster has:
- 8 nodes × 4 GB RAM = 32 GB total RAM
- 8 nodes × 4 cores = 32 total cores

→ You can process approx. 150–300 GB of data efficiently

Cluster Types
==============

1. Interactive Cluster:
- Manually turned ON/OFF
- Used during development or notebooks

Example:
If 5 notebooks each process 300 GB → total demand = 1.5 TB

2. Job Cluster:
- Auto-managed for production jobs (ADF, Airflow)
- Spun up on demand → shuts down after job

General Rule:
- Bigger data or more complex logic → bigger cluster needed

Data Format Performance:
- 200 GB Parquet is faster to process than 200 GB CSV (due to compression and schema)

Cluster Choice Depends On:
- Size of data
- Code complexity
- Required completion time
- Data compression format (CSV vs Parquet)
- Network bandwidth

Cluster Sizing Table
=====================

+------------+---------------+---------------+----------------+----------------+
|            |  Cluster A    |  Cluster B    |  Cluster C     |  Cluster D     |
+------------+---------------+---------------+----------------+----------------+
| Total RAM  |   400 GB      |   400 GB      |   400 GB       |   400 GB       |
| Total Cores|   160         |   160         |   160          |   160          |
| VM Type    |   X-Large     |   Large       |   Medium       |   Small        |
| Total VMs  |   1           |   2           |   4            |   8            |
| RAM/Exec   |   400 GB      |   200 GB      |   100 GB       |   50 GB        |
| Cores/Exec |   160 Cores   |   80 Cores    |   40 Cores     |   20 Cores     |
+------------+---------------+---------------+----------------+----------------+

Choosing a Cluster:
====================
Assume you have 4 TB of data.
→ 4000 GB / 10 ≈ 400 GB RAM needed.

You can choose:
- Cluster A → fewer, powerful VMs (better for wide transformations)
- Cluster D → more, smaller VMs (cheaper but slower)

💡 Tip:
- Cluster A is faster but more expensive
- Cluster D is cheaper but takes more time
