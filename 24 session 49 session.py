Session 49 – Partitioning in Spark
====================================

📌 Scenario:
Assume you have a table with a `country` column containing values like:
- India
- Japan
- Other countries

👉 It's a good idea to **partition the table** based on the `country` column.

Why?
----
- All rows for "India" will go into one partition
- All rows for "Japan" go into another
✅ This improves **query performance**, especially for filters like:
SELECT * FROM table WHERE country = 'India'

Reading and Writing a DataFrame:
--------------------------------
# Read a CSV file into DataFrame
df1 = spark.read.format("csv").option("header", "true").load("location")

# Write without partitioning
df1.write.save("location")

→ Data is saved (usually as Delta table in Databricks)
→ Stored as a **single Parquet file** without any partitioning

Writing with Partitioning:
---------------------------
df1.write.partitionBy("country").save("location")

✅ This writes the data **partitioned by the 'country' column**:
- Files are stored as:
  /location/country=India/
  /location/country=Japan/
  ...

📌 Important Distinction: 


parititons is disk memoery level 
and 
repartition is ram level computation 

- partitionBy() → Disk-level partitioning (affects storage layout)
- repartition() → In-memory partitioning (affects shuffle/distribution during processing)

When to Partition by a Column:
-------------------------------
- If you frequently filter on a column (e.g., WHERE country = 'India')
  → You should partition by that column

Best Practice:
--------------
Choose a column with **low cardinality** (fewer unique values) for partitioning.

✅ Good example: `department` (e.g., HR, IT, Sales)  
❌ Bad example: `id` (too many unique values → leads to too many small partitions)

Why?
- High cardinality partitioning results in:
  - File explosion in storage
  - Too many small files
  - Poor query performance
