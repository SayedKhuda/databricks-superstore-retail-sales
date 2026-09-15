# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline
# MAGIC
# MAGIC This section contains the PySpark pipeline used for the project.

# COMMAND ----------

# MAGIC %md
# MAGIC #####File path and imports

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    expr,
    coalesce,
    when,
    lit
)

import re

file_path = "/Volumes/mini_project_1/project1_schema/superstore_volume/superstore.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Read the raw CSV correctly

# COMMAND ----------

df_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("encoding", "ISO-8859-1")
    .csv(file_path)
)

display(df_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Check raw row count

# COMMAND ----------

print("Raw rows:", df_raw.count())
print("Raw columns:", len(df_raw.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC Check duplicates

# COMMAND ----------

total_rows = df_raw.count()
distinct_rows = df_raw.distinct().count()

print("Total rows:", total_rows)
print("Distinct rows:", distinct_rows)
print("Duplicate rows:", total_rows - distinct_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Standardise the column names

# COMMAND ----------

df_clean = df_raw

for old_col in df_clean.columns:
    new_col = old_col.strip().lower()
    new_col = re.sub(r'[^a-zA-Z0-9]+', '_', new_col)
    new_col = new_col.strip('_')

    df_clean = df_clean.withColumnRenamed(old_col, new_col)

print(df_clean.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Preserve the original dates

# COMMAND ----------

df_clean = (
    df_clean
    .withColumn("order_date_raw", col("order_date").cast("string"))
    .withColumn("ship_date_raw", col("ship_date").cast("string"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Correct the data types

# COMMAND ----------

df_clean = (
    df_clean

    # Order date - support multiple formats
    .withColumn(
        "order_date",
        coalesce(
            expr("try_to_date(order_date_raw, 'yyyy-MM-dd')"),
            expr("try_to_date(order_date_raw, 'dd/MM/yyyy')"),
            expr("try_to_date(order_date_raw, 'M/d/yyyy')")
        )
    )

    # Ship date - support multiple formats
    .withColumn(
        "ship_date",
        coalesce(
            expr("try_to_date(ship_date_raw, 'yyyy-MM-dd')"),
            expr("try_to_date(ship_date_raw, 'dd/MM/yyyy')"),
            expr("try_to_date(ship_date_raw, 'M/d/yyyy')")
        )
    )

    # Numeric columns
    .withColumn(
        "sales",
        expr("try_cast(sales as double)")
    )

    .withColumn(
        "quantity",
        expr("try_cast(quantity as int)")
    )

    .withColumn(
        "discount",
        expr("try_cast(discount as double)")
    )

    .withColumn(
        "profit",
        expr("try_cast(profit as double)")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Check the schema

# COMMAND ----------

df_clean.select(
    "order_date",
    "ship_date",
    "sales",
    "quantity",
    "discount",
    "profit"
).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Preview the cleaned data

# COMMAND ----------

display(
    df_clean.select(
        "order_id",
        "order_date",
        "ship_date",
        "customer_name",
        "product_name",
        "sales",
        "quantity",
        "discount",
        "profit"
    ).limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Data quality check

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum

important_columns = [
    "order_id",
    "order_date",
    "customer_id",
    "product_id",
    "sales",
    "quantity",
    "profit",
    "country",
    "region",
    "city",
    "category",
    "sub_category"
]

null_counts = df_clean.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in important_columns
])

display(null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Check the invalid date

# COMMAND ----------

display(
    df_clean
    .filter(col("order_date").isNull())
    .select(
        "order_id",
        "order_date_raw",
        "customer_id",
        "customer_name",
        "product_name",
        "sales",
        "profit"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### add rejection_reason in order_date, sales, quantity, and profit

# COMMAND ----------

df_validated = (
    df_clean
    .withColumn(
        "rejection_reason",
        when(
            col("order_date").isNull(),
            lit("Invalid order date")
        )
        .when(
            col("sales").isNull(),
            lit("Invalid sales value")
        )
        .when(
            col("quantity").isNull(),
            lit("Invalid quantity value")
        )
        .when(
            col("profit").isNull(),
            lit("Invalid profit value")
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Create rejected data

# COMMAND ----------

rejected_df = (
    df_validated
    .filter(col("rejection_reason").isNotNull())
)

print("Rejected rows:", rejected_df.count())

# COMMAND ----------

display(
    rejected_df.select(
        "order_id",
        "order_date_raw",
        "customer_name",
        "product_name",
        "sales",
        "profit",
        "rejection_reason"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Create valid Silver source

# COMMAND ----------

silver_source_df = (
    df_validated
    .filter(col("rejection_reason").isNull())
)

print("Valid rows:", silver_source_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #####Select the actual project fields

# COMMAND ----------

silver_df = silver_source_df.select(
    "order_id",
    "order_date",
    "ship_date",
    "ship_mode",
    "customer_id",
    "customer_name",
    "segment",
    "country",
    "city",
    "state",
    "postal_code",
    "region",
    "product_id",
    "category",
    "sub_category",
    "product_name",
    "sales",
    "quantity",
    "discount",
    "profit"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Verify Silver before saving

# COMMAND ----------

print("Silver rows:", silver_df.count())

silver_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Save Silver as a Delta table

# COMMAND ----------

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "mini_project_1.project1_schema.silver_superstore"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Save rejected records

# COMMAND ----------

(
    rejected_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "mini_project_1.project1_schema.rejected_superstore"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Verify both tables

# COMMAND ----------

silver_count = spark.table(
    "mini_project_1.project1_schema.silver_superstore"
).count()

rejected_count = spark.table(
    "mini_project_1.project1_schema.rejected_superstore"
).count()

print("Raw rows:      ", df_raw.count())
print("Silver rows:   ", silver_count)
print("Rejected rows: ", rejected_count)
print("Total checked: ", silver_count + rejected_count)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Display the Silver table

# COMMAND ----------

display(
    spark.table(
        "mini_project_1.project1_schema.silver_superstore"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer and Data Quality
# MAGIC
# MAGIC The Superstore CSV was ingested from a Unity Catalog Volume using PySpark.
# MAGIC
# MAGIC During ingestion, CSV quote and escape settings were configured to correctly
# MAGIC handle product descriptions containing commas and quotation marks.
# MAGIC
# MAGIC The following transformations were applied:
# MAGIC
# MAGIC - Standardised column names
# MAGIC - Converted order and shipping dates into DateType
# MAGIC - Converted sales, quantity, discount and profit into numerical data types
# MAGIC - Checked for duplicate records
# MAGIC - Validated important business fields
# MAGIC - Identified an invalid source date
# MAGIC - Separated rejected data from valid business data
# MAGIC - Stored validated records as a Delta Silver table
# MAGIC
# MAGIC ### Data Quality Results
# MAGIC
# MAGIC - Raw records: 9,994
# MAGIC - Duplicate records: 0
# MAGIC - Valid Silver records: 9,993
# MAGIC - Rejected records: 1
# MAGIC
# MAGIC The rejected record was retained separately for auditability rather than
# MAGIC silently modifying or deleting the source value.

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer - Business Metrics
# MAGIC
# MAGIC The Silver Delta table is used as the trusted source for calculating
# MAGIC business KPIs and analytical results required by stakeholders.

# COMMAND ----------

silver = spark.table(
    "mini_project_1.project1_schema.silver_superstore"
)

print("Silver rows:", silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Total Number of Customers
# MAGIC
# MAGIC This metric calculates the number of unique customers in the dataset.

# COMMAND ----------

from pyspark.sql.functions import countDistinct

total_customers = (
    silver
    .agg(
        countDistinct("customer_id")
        .alias("total_customers")
    )
)

display(total_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Total Number of Orders
# MAGIC
# MAGIC The total order count is calculated using distinct Order IDs because
# MAGIC one order can contain multiple products.

# COMMAND ----------

total_orders = (
    silver
    .agg(
        countDistinct("order_id")
        .alias("total_orders")
    )
)

display(total_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Total Sales
# MAGIC
# MAGIC This metric calculates the total value of all valid sales transactions.

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum, round

total_sales = (
    silver
    .agg(
        round(
            spark_sum("sales"),
            2
        ).alias("total_sales")
    )
)

display(total_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Total Profit
# MAGIC
# MAGIC This KPI represents the total profit generated across all valid transactions.

# COMMAND ----------

total_profit = (
    silver
    .agg(
        round(
            spark_sum("profit"),
            2
        ).alias("total_profit")
    )
)

display(total_profit)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Create one Gold KPI table

# COMMAND ----------

gold_kpis = (
    silver
    .agg(
        countDistinct("customer_id").alias("total_customers"),
        countDistinct("order_id").alias("total_orders"),
        round(spark_sum("sales"), 2).alias("total_sales"),
        round(spark_sum("profit"), 2).alias("total_profit")
    )
)

display(gold_kpis)

# COMMAND ----------

# MAGIC %md
# MAGIC ####SAVE

# COMMAND ----------

(
    gold_kpis.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "mini_project_1.project1_schema.gold_kpis"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####CHECK

# COMMAND ----------

display(
    spark.table(
        "mini_project_1.project1_schema.gold_kpis"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Top Sales by Country
# MAGIC
# MAGIC This analysis groups sales by country and ranks countries from highest to lowest total sales.

# COMMAND ----------

sales_by_country = (
    silver
    .groupBy("country")
    .agg(
        round(spark_sum("sales"), 2).alias("total_sales")
    )
    .orderBy(col("total_sales").desc())
)

display(sales_by_country)

# COMMAND ----------

# MAGIC %md
# MAGIC ####SAVE

# COMMAND ----------

(
    sales_by_country.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "mini_project_1.project1_schema.gold_sales_by_country"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Most Profitable Region and Country
# MAGIC
# MAGIC Profit is aggregated by country and region and ranked from highest to lowest.

# COMMAND ----------

profit_by_region_country = (
    silver
    .groupBy("country", "region")
    .agg(
        round(spark_sum("profit"), 2).alias("total_profit")
    )
    .orderBy(col("total_profit").desc())
)

display(profit_by_region_country)

# COMMAND ----------

# MAGIC %md
# MAGIC ####SAVE

# COMMAND ----------

(
    profit_by_region_country.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "mini_project_1.project1_schema.gold_profit_by_region_country"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Top Sales by Category
# MAGIC
# MAGIC This analysis compares total sales across product categories.

# COMMAND ----------

sales_by_category = (
    silver
    .groupBy("category")
    .agg(
        round(spark_sum("sales"), 2).alias("total_sales")
    )
    .orderBy(col("total_sales").desc())
)

display(sales_by_category)

# COMMAND ----------

# MAGIC %md
# MAGIC ####SAVE

# COMMAND ----------

(
    sales_by_category.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "mini_project_1.project1_schema.gold_sales_by_category"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Top 10 Sales Sub-Categories
# MAGIC
# MAGIC Sub-categories are ranked according to their total sales, with the top 10 retained.

# COMMAND ----------

top_10_subcategories = (
    silver
    .groupBy("sub_category")
    .agg(
        round(spark_sum("sales"), 2).alias("total_sales")
    )
    .orderBy(col("total_sales").desc())
    .limit(10)
)

display(top_10_subcategories)

# COMMAND ----------

# MAGIC %md
# MAGIC ####SAVE

# COMMAND ----------

(
    top_10_subcategories.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "mini_project_1.project1_schema.gold_top_10_subcategories"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Most Ordered Quantity Product
# MAGIC
# MAGIC Total ordered quantity is calculated for each product and ranked to identify the most ordered product.

# COMMAND ----------

product_by_quantity = (
    silver
    .groupBy("product_id", "product_name")
    .agg(
        spark_sum("quantity").alias("total_quantity")
    )
    .orderBy(col("total_quantity").desc())
)

display(product_by_quantity.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Top Ten

# COMMAND ----------

top_products_quantity = product_by_quantity.limit(10)

(
    top_products_quantity.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "mini_project_1.project1_schema.gold_top_products_quantity"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Top Customer Based on Sales and City
# MAGIC
# MAGIC Customer sales are aggregated by customer and city to identify the highest-value customers.

# COMMAND ----------

customer_sales = (
    silver
    .groupBy(
        "customer_id",
        "customer_name",
        "city"
    )
    .agg(
        round(spark_sum("sales"), 2).alias("total_sales")
    )
    .orderBy(col("total_sales").desc())
)

display(customer_sales.limit(10))

# COMMAND ----------

top_10_customers = customer_sales.limit(10)

(
    top_10_customers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "mini_project_1.project1_schema.gold_top_10_customers"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #Final check

# COMMAND ----------

tables = spark.sql("""
SHOW TABLES IN mini_project_1.project1_schema
""")

display(tables)