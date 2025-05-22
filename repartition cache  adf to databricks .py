Repartition & Cache in Spark – Part 1
======================================

🧱 Use Case:
Assume you have a large `employee` table with columns:
E_id, department, salary

Since the data is big and you want to perform a groupBy on `department`, you can apply:
df = df.repartition("department")

✅ This performs a **hash partition** based on `department`, ensuring all rows with the same department_id go to the same partition.  
This avoids shuffle during groupBy and improves performance.

Data Skew Example:
-------------------
Imagine a `population` table with country-wise records:
- India has billions of records
- USA and UK have millions

If you do:
df = df.repartition("country")

Then the partition for India will be huge (data skew), while USA and UK will be small.

➡️ To reduce skew, do:
df = df.repartition(5)
This redistributes data evenly, breaking up large skewed partitions.

💬 Interview Tip:
If asked "What is repartition?"  
→ Explain repartition and mention it’s commonly used to solve data skew.

If asked "What is data skew?"  
→ Use the population table example to explain how uneven distribution creates performance issues.

Why Use Repartition?
---------------------
✅ To fix data skew (balance data across partitions)  
✅ To increase partitions (if current ones are too large)  
✅ To reduce partitions (if too many small tasks are created)

⚠️ Remember:
- Each partition becomes one Spark task
- Ideal partition size = ~128MB

Repartition vs Coalesce
========================

🔁 Repartition:
- Always causes a **full shuffle** of the data
- Used when you want to **increase or re-distribute** partitions
- Example:
  df = df.repartition(10)
  df = df.repartition(10, "country")  # for grouping optimization

🧩 Coalesce:
- Reduces number of partitions **without shuffle**
- Faster than repartition
- Common in **merge operations**, or final steps of processing
- Example:
  df = df.coalesce(5)

Usage Rule:
-----------
- Use **repartition** for spreading or balancing data
- Use **coalesce** for reducing partitions efficiently

Cache & Persist in Spark – Part 2
==================================

By default, every transformation creates a new DataFrame.
That DataFrame is materialized in **RAM** (if possible), which is faster than disk.

⚠️ But RAM is limited.

✅ Use `df.cache()` when:
- You are reusing the same DataFrame multiple times
- You want Spark to try to keep it in memory

Note:
- `cache()` is a **hint**, not a guarantee.
- If memory is full, Spark may evict the cached data.

🧊 `persist()` is more powerful:
- Same as `cache()` but with control over storage levels.

Storage Levels:
---------------
- `MEMORY_ONLY` (default)
- `MEMORY_AND_DISK`
- `DISK_ONLY`
- `MEMORY_ONLY_SER` (serialized, takes less space)
- `MEMORY_AND_DISK_SER`

Example:
```python
from pyspark.storagelevel import StorageLevel
df.persist(StorageLevel.MEMORY_ONLY)











📌 Default for df.cache() = MEMORY_AND_DISK

Repartition Takes Time
⚠️ Repartitioning is an expensive operation due to full data shuffle.
Avoid unnecessary repartitioning.

When to Avoid Repartition?
If the DataFrame is used only 1–2 times

Even if skewed, repartitioning may not be worth the cost

Prefer to repartition only if the DataFrame is reused or critical for performance

Connecting Databricks Notebook to Azure Data Factory (ADF)
✅ In Databricks notebook:

python
Copy
Edit
# Accept parameter from ADF
filePath = dbutils.widgets.get("fileNameADF_parameter")
print(filePath)

# Read CSV from ADF-provided path
df = spark.read.csv(filePath)
display(df)

# Return a message back to ADF notebook activity output
dbutils.notebook.exit("Thank you, my notebook executed successfully")
✅ In ADF:

Add a Notebook Activity

Under Base Parameters, pass:
fileNameADF_parameter = "<your file path>"

Link to the above Databricks notebook

Use a new job cluster with access token for execution
