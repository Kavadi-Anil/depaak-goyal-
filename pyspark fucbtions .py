# 📌 Reading CSV in PySpark
df = spark.read.csv(location, header=True, inferSchema=True)

# Providing schema explicitly
order_schema = 'region STRING, country STRING'
df = spark.read.csv(location, schema=order_schema, header=True)

# Separator & Multiline CSV
df = spark.read.csv(location, sep=',', multiLine=True, header=True, inferSchema=True)


# 📌 Selecting Specific Columns
df1 = df.select("region", "order_id")
display(df1)


# 📌 Using Spark SQL
df.createOrReplaceTempView("order1")
spark.sql("""
    SELECT region, order_id 
    FROM order1 
    WHERE order_id > 100 
    ORDER BY region
""").show()


# 📌 Immutability of DataFrames
df1 = df.select("column1", "column2")  # Creates new instance
df = df.select("column1", "column2")   # Still creates new instance


# 📌 Selecting Columns in Different Ways
from pyspark.sql.functions import col, column
df.select("region", col("region"), column("region"), df.region).show()


# 📌 Expressions and Calculations
df1 = df.select("order_id", (col("totalprofit") / col("totalrevenue")).alias("profit_percentage"))

df.createOrReplaceTempView("orders")
df2 = spark.sql("""
    SELECT order_id, totalprofit / totalrevenue AS profit_percent 
    FROM orders
""")
display(df2)

from pyspark.sql.functions import expr
df3 = df.select("region", expr("totalprofit / totalrevenue AS profit_percent"))
display(df3)


# 📌 Adding a Column
from pyspark.sql.functions import lit
df4 = df.withColumn("status", lit("in progress"))
display(df4)


# 📌 Renaming & Dropping Columns
df5 = df.withColumnRenamed("region", "new_region")
df6 = df4.drop("status")


# 📌 Getting Columns List & Checking
columns_list = df.columns
if "status" in columns_list:
    print("Found")
else:
    print("Not found")


# 📌 Filtering (WHERE Conditions)
df7 = df.where("totalrevenue > 100000")
df8 = df.where(col("totalrevenue") > 100000)
df9 = df.where("totalrevenue > 100000 AND unitprice > 500")
df10 = df.filter((col("totalrevenue") > 100000) & (col("unitprice") > 500))


# 📌 Order By
df11 = df.orderBy("region")
display(df11)

df12 = df.orderBy(col("region").desc())
df12.show()


# 📌 Complex Query: Gross Profit
df.createOrReplaceTempView("orders")
df13 = spark.sql("""
    SELECT region, country, unitsold, (totalrevenue - totalcost) AS grossprofit
    FROM orders
    WHERE unitsold > 400
    ORDER BY unitsold
""")
display(df13)

df14 = df.where("unitsold > 400").select(
    "region", "country", "unitsold",
    expr("totalrevenue - totalcost AS grossprofit")
).orderBy("unitsold")
display(df14)


# 📌 String Functions
from pyspark.sql.functions import concat_ws, lower, upper, length, trim
df15 = df.select(
    concat_ws(" | ", "region", "country").alias("region_country"),
    lower(col("region")).alias("lower_region"),
    upper(col("region")).alias("upper_region"),
    length(col("region")).alias("length_region"),
    trim(col("region")).alias("trimmed_region")
)
display(df15)


# 📌 Limit & Union
df17 = df.limit(10)
df18 = df.limit(10)
df19 = df17.union(df18)  # Schema must match


# 📌 Distinct
df20 = df.select("region").distinct()


# 📌 Date and Time
from pyspark.sql.functions import current_date, current_timestamp, to_date, to_timestamp

df21 = df.withColumn("current_date", current_date()).withColumn("current_time", current_timestamp())

df22 = df.withColumn("string_date", lit("2024-02-02"))
df23 = df22.withColumn("converted_date", to_date("string_date"))

df24 = df.withColumn("string_date", lit("02-02-2024"))
df25 = df24.withColumn("converted_date", to_date("string_date", "dd-MM-yyyy"))

df26 = df.withColumn("string_datetime", lit("02-02-2024 12:00:00"))
df27 = df26.withColumn("converted_datetime", to_timestamp("string_datetime", "dd-MM-yyyy HH:mm:ss"))


# 📌 Date Parts & Differences
from pyspark.sql.functions import year, month, dayofmonth, date_add, date_sub, datediff

df28 = df.withColumn("current", current_date()).select("current").limit(5)

df29 = df28.select(
    year("current").alias("year"),
    month("current").alias("month"),
    dayofmonth("current").alias("day"),
    date_add("current", 7).alias("plus_7_days"),
    date_sub("current", 7).alias("minus_7_days"),
    datediff(current_date(), "current").alias("date_diff")
)


# 📌 Aggregations (Group By + Agg)
from pyspark.sql.functions import max, min, sum

df30 = df.groupBy("region").agg(
    max("totalrevenue").alias("max_rev"),
    min("totalrevenue").alias("min_rev"),
    sum("totalrevenue").alias("sum_rev")
)


# 📌 Sorting After Grouping
df31 = df.groupBy("region", "country").count().orderBy("region", "country")


# 📌 Null Handling
df32 = df.fillna({"currentdate": "2024-01-01"})
df33 = df.dropna(subset=["currentdate"])


# 📌 Joining Two DataFrames
df34 = df32.join(df33, df32["country"] == df33["country"], "left")
