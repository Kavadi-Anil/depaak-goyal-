Spark Join Optimization Techniques
==================================

1. Sort-Merge Join
------------------
After repartitioning, use `sortWithinPartitions()` to sort data within each partition.
This makes join operations faster by reducing shuffle and improving comparison efficiency.
It behaves like a narrow transformation and sorts data either in ascending or descending order.

Example:
df1 = df1.sortWithinPartitions("id")
df2 = df2.sortWithinPartitions("id")


2. Bucketing
------------
Bucketing is used to split data into fixed-size buckets based on a column.
It is applied **at the time of saving the data**.

Example:
df.write.format("parquet") \
  .bucketBy(2, "id") \
  .sortBy("id") \
  .option("path", "location") \
  .saveAsTable("table_name")

Explanation:
- `bucketBy(2, "id")` splits the data into 2 buckets by ID.
- Both tables must be bucketed on the **same column** and have the **same number of buckets** for join optimization.

Join Optimization with Bucketing:
- T1 bucketed on ID into B1, B2, B3
- T2 bucketed on ID into b1, b2, b3
- Join happens like:
  → B1 joins with b1
  → B2 joins with b2
  → B3 joins with b3

✅ This avoids full shuffle during join and speeds up execution.
🧠 Bucketing is a **disk-based optimization** because it physically writes partitioned files. 

 whenever u write into bucketed table then if someone reading it and doing operation on that table 
    it will be very fast bcz data is already optimised


3. AQE – Adaptive Query Execution (Spark 3.2+)
----------------------------------------------
AQE is an internal Spark feature that automatically improves performance **without code changes**.

Three key optimizations:

a) Dynamically Reducing Post-Shuffle Partitions:
   - When joins, aggregations, or orderBy operations cause many small shuffle partitions,
     AQE identifies and merges them into fewer large partitions.
   - This reduces the number of tasks and avoids unnecessary overhead.

b) Dynamically Switching Join Strategies:
   - Spark checks table sizes at runtime.
   - If a table becomes small (e.g., after a filter), AQE switches from sort-merge to broadcast join.

Example:
df1 = large table (1TB)  
df2 = large table (1TB)

→ Default join: Sort-Merge Join

df2 = df2.filter("country = 'Libya'") → becomes 500MB

df1.join(df2, df1.id == df2.id, "inner")

→ AQE will automatically switch to broadcast join based on the size of df2.

c) Skew Join Optimization:
   - If one partition (e.g., for "India") is much larger than others, it causes **data skew**.
   - AQE detects skew if:
     - Partition size > 256MB
     - OR it's 5× larger than the average partition size
   - Spark splits the skewed partition into smaller ones automatically to balance the load.


4. Quick Summary
----------------
1) **Broadcast Join** – Best when one table is significantly smaller than the other (T1 > T2 or T2 > T1)

2) **Repartition Before Join** – If both tables are large, repartition on join key before joining

3) **Sort Within Partition** – After repartitioning, apply sortWithinPartitions for faster joins

4) **Bucketing Join** – Pre-bucket the data during write phase for optimized disk-based joins

5) **AQE (Adaptive Query Execution)** – Built-in optimizer that:
   - Reduces small shuffle partitions
   - Dynamically chooses join strategy
   - Handles skew joins automatically

✅ AQE is **enabled by default** in Spark version 3.2 and above.
