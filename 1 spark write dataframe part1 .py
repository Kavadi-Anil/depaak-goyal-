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
