# ✅ Spark Write Modes (used in .mode("<mode>"))
# =============================================

# 1. overwrite  - Replaces existing data at the location/table
# 2. append     - Adds new data to existing files/tables
# 3. ignore     - Does nothing if data already exists (no error)
# 4. error (default) or errorifexists - Throws an error if data exists

# ===============================================================
# ✅ Spark DataFrame Writing
# ===============================================================

# Overwrite mode: replaces existing files
df.write.format("csv") \
    .mode("overwrite") \
    .option("path", "/batch/df1") \
    .save()

# 📌 Explanation:
# - Deletes existing data and writes new one
# - Creates 1 partition by default in DBFS


# Append mode: adds new data to existing files
df.write.format("csv") \
    .mode("append") \
    .option("path", "/batch/df1") \
    .save()


# ===============================================================
# ✅ Repartitioning for Large Data
# ===============================================================

# When data size is large, repartitioning improves performance
df2 = df1.repartition(10)

# Writing repartitioned DataFrame in Parquet format
df2.write.format("parquet") \
    .mode("overwrite") \
    .option("path", "/batch/df2") \
    .save()

# 📌 This will create 10 partitions in DBFS
# Helps optimize read/write performance with big data


# ===============================================================
# ✅ Reading Parquet Data
# ===============================================================

df3 = spark.read.parquet("/batch/df3")

# 📌 Reads all partitioned chunks from DBFS


# ===============================================================
# ✅ Saving as Table (Non-Delta Format)
# ===============================================================

# Save DataFrame as a managed Spark table (e.g., in Hive metastore)
df2.write.format("parquet") \
    .mode("overwrite") \
    .saveAsTable("T1")

# Query the saved table
spark.sql("SELECT * FROM T1").show()

# 📌 Notes:
# - Table is persisted and visible after restarting cluster
# - CRUD operations (DELETE, UPDATE) are NOT supported on non-Delta tables

# ❌ This will throw an error (Parquet format doesn't support DML)
spark.sql("DELETE FROM T1 WHERE region = 'Asia'")


# ===============================================================
# ✅ Delta Table (Supports DML)
# ===============================================================

# In Databricks, Delta format is the default if format isn't specified

# Save DataFrame as Delta Table
df2.write.format("delta") \
    .mode("append") \
    .saveAsTable("t3_delta")

# Now you can perform DML (CRUD) operations
spark.sql("DELETE FROM t3_delta WHERE region = 'Asia'")

# ✅ This works because the table is saved in Delta format 















Spark DataFrame Writing – Part 1
=================================

# Writing DataFrame to CSV
df.write.format("csv").mode("overwrite").option("path", "/batch/df1").save()

# Writing DataFrame to Parquet
df.write.format("parquet").mode("overwrite").option("path", "/batch/df1").save()

# Write Modes
-------------
mode("append")     → Adds new data to existing files if path exists  
mode("error")      → Throws an error if the path already exists  
mode("overwrite")  → Deletes existing data and writes new data  
mode("ignore")     → Silently skips writing if path exists (no error)

# Parquet format is preferred for performance and optimization.

# Example: Writing with append mode
df.write.format("parquet").mode("append").option("path", "/batch/df1").save()
# If run twice, it will append rows and create new files.

# Partitioning
df1 = df1.repartition(10)
# This splits the DataFrame into 10 partitions (files)
# Files will be written at: /batch/f1

# Reading all partitions:
df = spark.read.csv("/batch/f1")

# Reading a specific partition:
df = spark.read.csv("/batch/f1/part-00001")

Normal Spark Table (Non-Delta)
===============================

# Saving as a Parquet table (not Delta)
df.write.mode("overwrite").format("parquet").saveAsTable("T1")

# Also works without specifying format (still non-delta unless default is set)
df.write.mode("overwrite").saveAsTable("T1")

# These files are stored in: /user/hive/warehouse/T1/
# If df was repartitioned into 10 parts, you'll see 10 files there.

# Querying table:
spark.sql("SELECT * FROM T1").show()

# NOTE:
# Plain Parquet tables do NOT support DML (like DELETE or UPDATE)

# Appending to the same Parquet table:
df.write.mode("append").format("parquet").saveAsTable("T1")
# Running this twice will double the row count.

# This will fail for Parquet (non-delta):
spark.sql("DELETE FROM T1 WHERE region = 'Asia'")  ❌ (Not supported)

Delta Format (Delta Table)
===========================

# In Databricks, if you don't mention format, it defaults to Delta
df.write.mode("overwrite").saveAsTable("T1")

# Or explicitly:
df.write.mode("overwrite").format("delta").saveAsTable("T1")

# Delta table supports DML operations like DELETE, UPDATE
spark.sql("DELETE FROM T1 WHERE region = 'Asia'")  ✅ (Works)

# Delta also supports:
# - ACID transactions
# - Time travel
# - Schema enforcement
# - Merge (UPSERT) operations

