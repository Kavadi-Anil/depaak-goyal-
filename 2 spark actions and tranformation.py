# 🔥 Apache Spark – Transformations & Architecture (Databricks Notes)

---

## 🔹 What Are Partitions in Spark?

When you load a **large file** into Spark, it is automatically **split into chunks in memory**.  
These **chunks are called partitions**.

- Each partition is processed independently in parallel.
- Partitioning helps Spark distribute work across the cluster efficiently.

---

## 🔹 What is RDD?

**RDD** (Resilient Distributed Dataset) is the fundamental data structure in Spark.

- `df.select()` ➝ DataFrame
- Internally: `df.select()` ➝ RDD ➝ Partitions

> Everything in Spark — whether it’s a DataFrame, Delta table, or Dataset — is eventually converted into **RDD** under the hood.

### ✅ RDD Properties:
- **Resilient**: If a node crashes, Spark can recompute lost data using lineage info.
- **Distributed**: Data is distributed across a cluster for parallel processing.

---

## 🔹 Spark Architecture Overview

| Component        | Description |
|------------------|-------------|
| **Driver**       | Orchestrates execution; talks to Cluster Manager |
| **Cluster Manager** | Allocates resources across the cluster |
| **Executors (Worker Nodes)** | Perform actual computation; run tasks using allocated CPU & memory |

---

### 🧠 Cluster Manager Types

- **Standalone** (Default in Databricks)
- **YARN** – Yet Another Resource Negotiator
- **Mesos**
- **Kubernetes**

---

## 🔹 Spark UI – Jobs & DAG

Whenever you execute code in Databricks, **Spark creates jobs** behind the scenes.

### Example:
```python
df = spark.read.option("header", True).csv("path.csv")
df_filtered = df.filter("unitprice > 100")
display(df_filtered)
```

### You’ll see:
1. **Job 1** – For inferring schema
2. **Job 2** – For reading CSV
3. **Job 3** – For display/show (action)

### DAG Visualization:
- You can inspect jobs visually (DAG graph) in the Spark UI.
- Click on the job to see how tasks are broken down.

### Partitions & Tasks:
If you did:
```python
df_re = df.repartition(10)
```
You would see **10 tasks** in Spark UI — one for each partition.

---

## 🔹 Lazy Evaluation in Spark

Spark **doesn't execute** transformations immediately. It builds a logical plan (DAG).

Only when an **action** (like `.show()`, `.count()`, `.write()`) is called, Spark executes the plan.

---

## 🔹 Actions vs. Transformations

| Transformations       | Actions             |
|------------------------|---------------------|
| `select()`, `filter()` | `show()`, `count()` |
| `groupBy()`, `orderBy()` | `write()`, `display()` |

Transformations are **lazy**, actions **trigger execution**.

---

## 🔹 Types of Transformations in Spark

### 1. **Narrow Transformations**
- Examples: `filter()`, `map()`
- Operate **within a single partition**
- **No data shuffling**
- Fast, parallel, efficient 

Narrow Transformation
======================

Each input partition contributes to only one output partition.
No shuffle occurs between nodes.

Example: map(), filter(), sortWithinPartitions()

+-------------+       +-------------+
| Partition 1 | ----> | Partition 1 |
+-------------+       +-------------+

+-------------+       +-------------+
| Partition 2 | ----> | Partition 2 |
+-------------+       +-------------+

+-------------+       +-------------+
| Partition 3 | ----> | Partition 3 |
+-------------+       +-------------+

✅ Fast and efficient — no data movement across the cluster.




  

### 2. **Wide Transformations**
- Examples: `groupBy()`, `orderBy()`, `join()`
- **Require data from multiple partitions**
- Cause **shuffling** across the network
- Slower due to **inter-partition dependency**

### ⚠️ Wide transformations should be minimized for performance.



Wide Transformation
====================

Each input partition can contribute to multiple output partitions.
Spark must **shuffle** data between executors.

Example: groupByKey(), join(), distinct(), repartition()

+-------------+       +-------------+
| Partition 1 | --->  | Partition A |
+-------------+     / +-------------+
                    /
+-------------+    /  +-------------+
| Partition 2 | --+--> | Partition B |
+-------------+    \  +-------------+
                    \
+-------------+     \ +-------------+
| Partition 3 | --->  | Partition C |
+-------------+       +-------------+

⚠️ Involves shuffling → network + disk I/O → more expensive




  

---

## 🔁 What is Shuffling?

Shuffling happens **during wide transformations** when:
- Spark has to **reorganize and move data** between partitions
- Costly and involves disk & network I/O

---

## 🔹 DAG Stages in Spark

### Stage
A **stage** is a set of transformations that can run **in parallel** without shuffling.

- **Narrow transformations** ➝ Single stage
- **Wide transformations** ➝ Introduce **shuffling**, hence a **new stage** is created

---

## 🔍 Example Code: Lazy Evaluation + Transformation Behavior

```python
df2 = df.filter("unitprice > 300")
df3 = df.filter("unitprice > 200")
df4 = df.filter("unitprice > 100")

df4.explain()
```

- The plan shows only the last filter `unitprice > 100`
- Spark **optimizes** and removes unused filters (`df2`, `df3`)
- Until you call `display(df4)` or `.show()`, **no job** is executed

---

## 🧠 Summary

- **Everything becomes RDD** ➝ operates on partitions
- **Transformations** are lazy and logical
- **Actions** trigger execution
- Prefer **narrow** transformations for performance
- Wide transformations cause **shuffling**, create new **stages**
- DAG, jobs, stages, and tasks can be explored in **Spark UI**








                                                    

## Spark Transformations

When working with large files in Spark, the file is divided into smaller chunks called **partitions** in memory.

### RDD (Resilient Distributed Dataset)

- Suppose you have an input file with 10 partitions.  
- **Resilient** means that even if data is lost, Spark (or Databricks) can recover it.  
- **Distributed** means the file is split and processed across multiple nodes in the cluster.

Everything in Spark (DataFrame, Delta Table, etc.) is eventually converted into an RDD.

Example:  
`df.select` → `rdd.select` → `partitions.select`

---

## Spark Architecture

- **Spark Driver**: Connects to the cluster manager and requests resources.
- **Cluster Manager**: Manages worker nodes and decides how much work each node should get.
- **Executor Nodes (Worker Nodes)**: Physical memory and CPU where tasks run.

### Types of Cluster Managers

- **Standalone** (default in Databricks)  
- **YARN** (Yet Another Resource Negotiator)  
- **Mesos**  
- **Kubernetes**

---

## Spark UI

### Jobs

When you run code in Databricks, it creates **jobs** for each task, like:

1. Inferring schema  
2. Reading data from CSV  
3. Displaying data

If you click on a job, you can view the **DAG (Directed Acyclic Graph)** and **tasks**.

Example:  
If you use `repartition(10)` and run the job, Spark UI will show **10 tasks** because there are 10 partitions.

### Example: Lazy Evaluation in Filters

```python
df2 = df.filter("unitprice > 300")
df3 = df.filter("unitprice > 200")
df4 = df.filter("unitprice > 100")

df4.explain()
```

- Spark will not execute the filters for `>300` and `>200` since they are not used in an action.
- Only the last filter (`>100`) is included in the plan.
- Spark is smart enough to optimize and apply only necessary filters.

```python
display(df4)
```

- Now Spark will create a job because `display()` is an **action**.

---

## Spark Operations

Spark operations are of two types:

### 1. Transformations  
Examples: `select`, `filter`, `orderBy`, `groupBy`  
- These are **lazy** — they don't run until an action is triggered.

### 2. Actions  
Examples: `show`, `display`, `count`, `write`  
- These **trigger execution**.

---

## Types of Transformations

### Narrow Transformations

- Each partition is processed **independently**.
- Example: `filter`
- **Fast** because no data is shared between partitions.

### Wide Transformations

- Needs data from **other partitions**.
- Examples: `groupBy`, `orderBy`, `join`
- **Slow** due to **data shuffling** across partitions.

---

### What is Shuffling?

- Happens during wide transformations.
- Data is moved across partitions to complete the operation.
- Shuffling slows down performance.

**Tip**: Avoid wide transformations when possible — they cause interdependency between partitions.

---

## Lazy Evaluation in Spark

- Spark does **not** execute transformations immediately.
- Only when an **action** is called, Spark builds a plan and executes only the required transformations.

---

## DAG and Stages in Spark

- **Stage**: A group of tasks that can run **in parallel**.
- If all transformations are narrow, only **one stage** is created.
- When a **shuffle** happens (in wide transformations), **another stage** is created.

---





Narrow and Wide Transformation – Diagram Representation
=========================================================

📄 Table Name: temperature

| city     | temp | country |
|----------|------|---------|
| Mumbai   | 20   | India   |
| Chennai  | 10   | India   |
| Delhi    | 57   | India   |
| Mumbai   | 12   | India   |
| Chennai  | 30   | India   |
| Chennai  | 50   | India   |
| Delhi    | -20  | India   |
| Mumbai   | 25   | India   |

🧪 DataFrame Queries and Transformation Type:
--------------------------------------------

1. Narrow Transformation:
--------------------------
✅ These do NOT cause a shuffle. They operate within the same partition.

df1 = df.filter("temp < 50")               # Narrow: filters rows in place
df2 = df1.select("city", "temp")           # Narrow: selects columns without moving data

2. Wide Transformation:
------------------------
⚠️ These DO cause a shuffle. They require moving data between partitions.

df3 = df2.orderBy("temp")                  # Wide: requires global sorting, triggers shuffle

📝 Summary:
----------
- Narrow transformations: filter(), select()
- Wide transformations: orderBy(), groupBy(), join(), distinct()

Only when orderBy("temp") is called, a shuffle happens across partitions — making it a wide transformation.


------------------ DIAGRAM BELOW ------------------

                Partition Table 1               Partition Table 2               Partition Table 3
              +---------------------+         +---------------------+         +---------------------+
              |  Mumbai   20  India |         |  Mumbai   12  India |         |  Delhi  -20  India  |
              |  Chennai  10  India |         |  Chennai 30  India |         |  Mumbai   25  India |
              |  Delhi    57  India |         |  Chennai 50  India |         |                     |
              +---------------------+         +---------------------+         +---------------------+
                        |                               |                               |
       df1 = df.filter("temp" < 50)       df1 = df.filter("temp" < 50)       df1 = df.filter("temp" < 50)
                        |                               |                               |
              +---------------------+         +---------------------+         +---------------------+
              |  Mumbai   20  India |         |  Mumbai   12  India |         |  Delhi  -20  India  |
              |  Chennai  10  India |         |  Chennai 30  India |         |  Mumbai   25  India |
              +---------------------+         +---------------------+         +---------------------+
                        |                               |                               |
 df2 = df1.select("city", "temp")   df2 = df1.select("city", "temp")   df2 = df1.select("city", "temp")
                        |                               |                               |
              +------------------+              +------------------+              +------------------+
              |  Mumbai   20     |              |  Mumbai   12     |              |  Delhi  -20      |
              |  Chennai  10     |              |  Chennai 30      |              |  Mumbai   25     |
              +------------------+              +------------------+              +------------------+
                        \                               |                               /
                         \___till above only narrow transformations happened____________/
                                             |
                             df3 = df2.orderBy("temp")  → wide transformation starts
                                             |
                                    +----------------------------+
                                    |        Final Output        |
                                    +----------------------------+
                                    |  Delhi   -20               |
                                    |  Chennai 10                |
                                    |  Mumbai  12                |
                                    |  Mumbai  20                |
                                    |  Mumbai  25                |
                                    |  Chennai 30                |
                                    +----------------------------+
