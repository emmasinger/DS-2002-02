# Databricks notebook source
# MAGIC %md
# MAGIC ### Processing Incremental Updates with Structured Streaming and Delta Lake
# MAGIC In this lab you'll apply your knowledge of structured streaming and Auto Loader to implement a simple multi-hop architecture.
# MAGIC 
# MAGIC #### 1.0. Import Shared Utilities and Data Files
# MAGIC Run the following cell to setup necessary variables and clear out past runs of this notebook. Note that re-executing this cell will allow you to start the lab over.

# COMMAND ----------

# MAGIC %run ./Includes/5.1-Lab-setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 2.0. Bronze Table: Ingest data
# MAGIC This lab uses a collection of customer-related CSV data from DBFS found in */databricks-datasets/retail-org/customers/*.  Read this data using Auto Loader using its schema inference (use **`customersCheckpointPath`** to store the schema info). Stream the raw data to a Delta table called **`bronze`**.

# COMMAND ----------

# TODO:

customersCheckpointPath = f"{DA.paths.checkpoints}/customers"

query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", customersCheckpointPath)
    .load("/databricks-datasets/retail-org/customers/")
    .writeStream
    .format("delta")
    .option("checkpointLocation", customersCheckpointPath)
    .outputMode("append")
    .table("bronze"))


# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.1. Create a Streaming Temporary View
# MAGIC Create a streaming temporary view into the bronze table so that we can perform transformations using SQL.

# COMMAND ----------

(spark
  .readStream
  .table("bronze")
  .createOrReplaceTempView("bronze_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.2. Clean and Enhance the Data
# MAGIC Use the CTAS syntax to define a new streaming view called **`bronze_enhanced_temp`** that does the following:
# MAGIC * Skips records with a null **`postcode`** (set to zero)
# MAGIC * Inserts a column called **`receipt_time`** containing a current timestamp
# MAGIC * Inserts a column called **`source_file`** containing the input filename

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO:
# MAGIC CREATE OR REPLACE TEMPORARY VIEW bronze_enhanced_temp AS
# MAGIC   SELECT
# MAGIC     *
# MAGIC     , current_timestamp() AS receipt_time
# MAGIC     , input_file_name() AS source_file
# MAGIC     FROM bronze_temp
# MAGIC     WHERE postcode > 0

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.0. Silver Table
# MAGIC Stream the data from **`bronze_enhanced_temp`** to a table called **`silver`**.

# COMMAND ----------

# TODO:
silverCheckpointPath = f"{DA.paths.checkpoints}/silver"

query = (spark.table("bronze_enhanced_temp")
              .writeStream
              .format("delta")
              .option("checkpointLocation", silverCheckpointPath)
              .outputMode("append")
              .table("silver"))

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3.1. Create a Streaming Temporary View
# MAGIC Create another streaming temporary view for the silver table so that we can perform business-level queries using SQL.

# COMMAND ----------

(spark
  .readStream
  .table("silver")
  .createOrReplaceTempView("silver_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 4.0. Gold Table
# MAGIC Use the CTAS syntax to define a new streaming view called **`customer_count_temp`** that counts customers per state.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO:
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_count_by_state_temp AS
# MAGIC   SELECT state
# MAGIC   , count(state) AS customer_count
# MAGIC   FROM silver_temp
# MAGIC   GROUP BY state

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, stream the data from the **`customer_count_by_state_temp`** view to a Delta table called **`gold_customer_count_by_state`**.

# COMMAND ----------

# TODO:

customerCountsCheckpointPath = f"{DA.paths.checkpoints}/customer_counts"

query = (spark.table("customer_count_by_state_temp")
              .writeStream
              .format("delta")
              .option("checkpointLocation", customerCountsCheckpointPath)
              .outputMode("complete")
              .table("gold_customer_count_by_state"))

# COMMAND ----------

DA.block_until_stream_is_ready(query)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.0. Query the Results
# MAGIC Query the **`gold_customer_count_by_state`** table (this will not be a streaming query). Plot the results as a bar graph and also using the map plot.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_customer_count_by_state

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6.0. Clean Up
# MAGIC Run the following cell to remove the database and all data associated with this lab.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC By completing this lab, you should now feel comfortable:
# MAGIC * Using PySpark to configure Auto Loader for incremental data ingestion
# MAGIC * Using Spark SQL to aggregate streaming data
# MAGIC * Streaming data to a Delta table
