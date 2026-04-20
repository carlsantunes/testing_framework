# Databricks notebook source
# MAGIC %md
# MAGIC ## Development Tests
# MAGIC This notebook provides a series of pre-defined methods to check:
# MAGIC - Documentation
# MAGIC - Notebook structure
# MAGIC - Table data
# MAGIC
# MAGIC To run tests fill the widgets above. 
# MAGIC
# MAGIC Widgets not filled will make the checks of the corresponding section to be skipped.
# MAGIC
# MAGIC **Jira Issue:** 
# MAGIC
# MAGIC **Author:**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize

# COMMAND ----------

# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

import requests # Make API requests
import json # Manage JSON content
import time # Process times
from datetime import datetime, timezone
from requests.auth import HTTPBasicAuth # Authentication for API requests
import sys # To exit execution

from pyspark.sql.types import StructField
from functools import reduce

# COMMAND ----------

# MAGIC %md
# MAGIC File with the log definition

# COMMAND ----------

# MAGIC %run "/Users/cvantunes@ext.worten.pt/Automation/src/utils/logging_utils"

# COMMAND ----------

# MAGIC %md
# MAGIC File with the definition of configurable global variables like tokens and namespaces

# COMMAND ----------

# MAGIC %run "/Users/cvantunes@ext.worten.pt/Automation/Config File"

# COMMAND ----------

# MAGIC %md
# MAGIC File with methods to interact with databricks (create jobs, start clusters, create notebooks, etc)

# COMMAND ----------

# MAGIC %run "/Workspace/Users/cvantunes@ext.worten.pt/Automation/src/utils/databricks_utils"

# COMMAND ----------

# MAGIC %md
# MAGIC File with Quality Assurance class and all its checks

# COMMAND ----------

# MAGIC %run "/Workspace/Users/cvantunes@ext.worten.pt/Automation/src/utils/quality_assurance_utils"

# COMMAND ----------

# MAGIC %md
# MAGIC Class to interact with Atlassian products (Confluence and Jira). Reads documentation, reads and creates Jira issues, etc.

# COMMAND ----------

# MAGIC %run "/Users/cvantunes@ext.worten.pt/Automation/src/utils/atlassian_utils"

# COMMAND ----------

# MAGIC %md
# MAGIC Class to check the quality of documentation with respect to the governance guidelines

# COMMAND ----------

# MAGIC %run "/Users/cvantunes@ext.worten.pt/Automation/src/utils/governance_utils"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Widgets

# COMMAND ----------

# Get Confluence Documentation Page URL
dbutils.widgets.text("Confluence URL", "")
confluence_url = dbutils.widgets.get("Confluence URL")

# Get full table name
dbutils.widgets.text("Full Table Name", "")
full_dev_table_name = dbutils.widgets.get("Full Table Name")

# Get notebook path
dbutils.widgets.text("Notebook Path", "")
notebook_path = dbutils.widgets.get("Notebook Path")

log_info("Widgets created")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameter data

# COMMAND ----------

# Convert full table name to array [catalog, schema, table]
list_full_table_name = full_dev_table_name.split(".")
if len(list_full_table_name) != 3:
    log_error("Table does not have catalog, schema and table format")
if 'prd' in list_full_table_name[0]:
    list_full_table_name[0] = list_full_table_name[0].replace("prd", "dev")
    full_dev_table_name = full_dev_table_name.replace("prd", "dev")

# store catalog, schema and table in different variables
catalog_dev = list_full_table_name[0]
catalog_prd = list_full_table_name[0].replace("dev", "prd")
schema = list_full_table_name[1]
table = list_full_table_name[2]
view = 'vw_' + table

# Get full view name for dev and prd
full_view_dev_name = catalog_dev + '.' + schema + '.' + view
full_view_prd_name = catalog_prd + '.' + schema + '.' + view

# Get notebook name
notebook_name = 'trf_' + table

# If an asset is the people catalog consider it an HR table
flg_is_HR_table = 1 if 'people' in catalog_dev else 0

# When no notebook path provided, consider the notebook in the repository dev main branch
if notebook_path == '':
    notebook_path = f'/Repos/dbw-dev-soi/Databricks-dev/Transformation_Notebooks/trf_{table}'

if confluence_url == '':
    flg_check_doc_gov = False
else:
    flg_check_doc_gov = True

log_info("Parameters read")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cluster to execute jobs

# COMMAND ----------

#cluster_id = '1127-122708-sduum220' # Shared Expl HR
cluster_id = '1017-110510-35o7hj6e' # Shared Expl UC HR
#cluster_id = '1003-150401-9z4805bt' # Shared Expl UC -> Does not support job workloads

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manage Databrics
# MAGIC Class that has methods to interact with Databricks API, like for creating notebooks and run jobs.

# COMMAND ----------

md = ManageDatabricks(
  cfg.databricks_token, 
  cfg.databricks_instance,
  cfg.atlassian_token,
  cfg.user_email
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manage Quality Assurance
# MAGIC Class with methods to check the quality (testing) of tables and views. There is one object per asset to test.

# COMMAND ----------

qa = ManageQualityAssurance(catalog_dev, schema, table)
#qa = ManageQualityAssurance(catalog_prd, schema, table) # Comment this line if this asset does not exist in prd yet

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manage Confluence
# MAGIC Class to interact with the Atlassian API (Confluence and Jira). We can read table design pages.

# COMMAND ----------

if flg_check_doc_gov:
  confluence = ManageAtlassian(
    cfg.atlassian_token,
    cfg.user_email)

  page_title, page_html_body = confluence.get_page(confluence_url)

  documentation = confluence.map_design_to_notebook(
    page_title = page_title, 
    page_body = page_html_body,
    confluence_url = confluence_url)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manage Governance
# MAGIC Class to check the quality of the use of governance guidelines in documentation

# COMMAND ----------

if flg_check_doc_gov:
  doc_gov = ManageGovernance(documentation)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Documentation

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: column prefixes and order

# COMMAND ----------

if flg_check_doc_gov:
  doc_gov.check_columns_prefixes_and_order()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: table has primary key

# COMMAND ----------

if flg_check_doc_gov:
  doc_gov.check_at_least_one_columns_is_primary_key()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: columns have valid data type according to column name

# COMMAND ----------

if flg_check_doc_gov:
  doc_gov.check_columns_have_valid_data_type() 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Notebook Structure

# COMMAND ----------

notebook_content = md.get_notebook_content(notebook_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: Notebook template

# COMMAND ----------

if isinstance(notebook_content, str):
  qa.check_notebook_follows_template(notebook_content)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: Prints and Displays in notebook

# COMMAND ----------

if isinstance(notebook_content, str):
  qa.check_prints_displays(notebook_content)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: coalesce(..., -1) for suks and buks

# COMMAND ----------

#qa.check_coalesce(notebook_content) # function not fully tested

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: recreateView Must be = to False

# COMMAND ----------

#qa.check_recreate_view(notebook_content) # function not fully tested

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check data asset

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: Count rows

# COMMAND ----------

qa.count_rows()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: QA Utils

# COMMAND ----------

# def replace_backtick_alias_with_udfdecrypt_column(sql_code):
#   # Pattern matches: udfDecrypt(column_name, ...) ... AS `alias`
#   pattern = r"udfDecrypt\(\s*(\w+)\s*,.*?\)\s*.*?as\s+`([^`]+)`"

#   def replacer(match):
#     original_column = match.group(1)  # e.g. teste1
#     return match.group(0).replace(f"`{match.group(2)}`", original_column)

#   return re.sub(pattern, replacer, sql_code, flags=re.IGNORECASE)

# COMMAND ----------

# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType
# from cryptography.fernet import Fernet

# # Replace this with your actual key retrieval logic
# fernet_key = dbutils.secrets.get(scope="hr-analytics", key="fernet-key")
# fernet = Fernet(fernet_key.encode())

# def decrypt_value(encrypted_value):
#     if encrypted_value is None:
#         return None
#     try:
#         return fernet.decrypt(encrypted_value.encode()).decode()
#     except Exception:
#         return None

# decrypt_udf = udf(decrypt_value, StringType())

# COMMAND ----------

# if flg_is_HR_table == 'Yes':
#   # Parameters
#   test_table_name = f"{full_table_name}_qa_tests"

#   # Query the table comment
#   table_comment_query = f"""
#   SELECT comment
#   FROM {catalog_dev}.information_schema.tables
#   WHERE table_schema = '{schema}'
#     AND table_name = '{table}'
#   """

#   comment_df = spark.sql(table_comment_query)
#   table_comment = comment_df.collect()[0]['comment'] if comment_df.count() > 0 else log_error("Table has no comment")

#   # 1. Extract schema and comments from the view
#   schema_view_query = f"""
#   SELECT column_name, data_type
#   FROM system.information_schema.columns
#   WHERE table_schema = '{schema}'
#     AND table_name = '{view}'
#   ORDER BY ordinal_position
#   """
#   df_schema_view = spark.sql(schema_view_query).withColumnRenamed("column_name", "view_column_name")

#   schema_table_query = f"""
#   SELECT column_name, comment
#   FROM system.information_schema.columns
#   WHERE table_schema = '{schema}'
#     AND table_name = '{table}'
#   ORDER BY ordinal_position
#   """
#   df_schema_table = spark.sql(schema_table_query)

#   # Add a row index to preserve order
#   df_schema_view_indexed = df_schema_view.withColumn("row_id", monotonically_increasing_id())
#   df_schema_table_indexed = df_schema_table.withColumn("row_id", monotonically_increasing_id())

#   # Join on row_id
#   joined_df = df_schema_view_indexed.join(df_schema_table_indexed, on="row_id").drop("row_id").collect()

#   # Mapping Spark → Databricks SQL
#   spark_to_sql_type = {
#     "BYTE": "TINYINT",
#     "ShortType": "SMALLINT",
#     "INT": "INT",
#     "LONG": "BIGINT",
#     "FloatType": "FLOAT",
#     "DoubleType": "DOUBLE",
#     "DecimalType": "DECIMAL",
#     "STRING": "STRING",
#     "BinaryType": "BINARY",
#     "BooleanType": "BOOLEAN",
#     "TIMESTAMP": "TIMESTAMP",
#     "DateType": "DATE"
#   }

#   # 2. Build CREATE TABLE statement with column comments
#   columns_def = []
#   for row in joined_df:
#     col_name = row['column_name']
#     col_type = spark_to_sql_type.get(row['data_type'])
#     col_comment = row['comment'] or ''
#     columns_def.append(f"{col_name} {col_type} COMMENT '{col_comment}'")

#   columns_sql = " , ".join(columns_def)

#   create_table_sql = f"""CREATE OR REPLACE TABLE {test_table_name} ({columns_sql}) COMMENT '{table_comment}'"""

#   spark.sql(create_table_sql)

#   # Fetch tags from source table
#   tags_df = spark.sql(f"""
#   SELECT tag_name, tag_value
#   FROM system.information_schema.table_tags
#   WHERE catalog_name = '{catalog_dev}'
#     AND schema_name  = '{schema}'
#     AND table_name   = '{table}'
#   """)  # Returns one row per (tag_name, tag_value)

#   for row in tags_df.collect():
#     key = row["tag_name"]
#     val = row["tag_value"]
#     sql = f"ALTER TABLE {test_table_name} SET TAGS ('{key}' = '{val}')"
#     spark.sql(sql)

#   df_source_table = spark.read.table(f"{full_dev_table_name}")
#   for row in joined_df:
#     col_name = row['column_name']
#     col_type = spark_to_sql_type.get(row['data_type'])
#     if col_name in ('dtm_created_at', 'dtm_updated_at'):
#       continue
#     else:
#       df_source_table = df_source_table.withColumn(col_name, decrypt_udf(df_source_table[col_name]).cast(col_type))
#   df_source_table.write.mode("overwrite").saveAsTable(f"{test_table_name}")

#   full_table_name_for_qa = test_table_name
#   qa_utils_notebook_template_path = f"/Users/{cfg.user_email}/Important Notebooks/HR QA Utils - [NOME DO NOTEBOOK]"
# else:
#   qa_utils_notebook_template_path = f"/Users/{cfg.user_email}/Important Notebooks/QA Utils - [NOME DO NOTEBOOK]"
#   full_table_name_for_qa = full_dev_table_name

# df_table = spark.read.table(f"{full_table_name_for_qa}")

# COMMAND ----------

# qa_utils_template = md.get_notebook_content(notebook_path = qa_utils_notebook_template_path)

# COMMAND ----------

# qa_utils = md.generate_qa_utils_notebook_string_format(qa_utils_template_string = qa_utils_template, 
#                                             full_table_name = full_table_name_for_qa)

# COMMAND ----------

# qa_notebook_path = f"/Users/{cfg.user_email}/Tests/Tests QA/QA Utils - {notebook_name}"
# md.create_notebook(qa_utils, qa_notebook_path)

# COMMAND ----------

# md.start_cluster(cluster_id)

# COMMAND ----------

# run_id = md.start_job(qa_notebook_path, cluster_id)

# COMMAND ----------

#md.has_job_finished(run_id)
#md.get_job_result(run_id)

# COMMAND ----------

# MAGIC %pip install beautifulsoup4

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/dbw-dev-soi/Databricks-dev/Quality_Assurance/Framework_Teste/QA_Utils"

# COMMAND ----------

validator = TableValidator(full_dev_table_name, cfg.atlassian_token)

# COMMAND ----------

validator.compare_governance_current_confluence_version()

# COMMAND ----------

validator.checks_confluence()

# COMMAND ----------

validator.check_table()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: Dimension table has suk with -1 and -2

# COMMAND ----------

qa.check_dim_suk_1_2()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: suks with -1 and -2

# COMMAND ----------

qa.print_suks_1_2()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: Different suks have different values

# COMMAND ----------

qa.suks_represent_unique_rows() # function not fully tested - Need to create unit tests

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: Find same BUKs with SCD validity intervals overlaps

# COMMAND ----------

qa.scd_validity_intervals_overlap(granularity='day') # function not fully tested - needs unit tests

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from general_data_prd.dw.dim_country
# MAGIC order by buk_country, dtm_scd_valid_from

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: Key cannot be null

# COMMAND ----------

qa.key_nulls()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: Find pk duplicates

# COMMAND ----------

qa.key_duplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: Distinct columns

# COMMAND ----------

qa.distinct_values_for_each_column()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: Max and Min of each column

# COMMAND ----------

qa.max_min_values_for_each_column()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: Sum columns

# COMMAND ----------

qa.sums()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: Null or blank cells in table

# COMMAND ----------

qa.null_or_blank()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: Distinct suk, buk
# MAGIC For each suk there should be only one distinct buk value

# COMMAND ----------

qa.distinct_suk_buk()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: Each dim PK corresponds to one and only one suk

# COMMAND ----------

qa.dim_pk_suk()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: Compare PRD table to DEV
# MAGIC This section needs to be manually adapted for every test

# COMMAND ----------

df_table_dev = qa.get_df()
qa_prd = ManageQualityAssurance(catalog_prd, schema, "fac_sls_online_order_line_deliveries")
df_table_prd = qa_prd.get_df(version=1868)

# COMMAND ----------

df_dev = (
  df_table_dev
  .drop("dtm_created_at","dtm_updated_at")
  .filter(
    (col("suk_date_created") < 20260201) &
    (col("suk_date_delivered") < 20260201) &
    (col("suk_date_paid") < 20260201) &
    (col("suk_date_shipped") < 20260201) &
    (col("suk_date_cancelled") < 20260201) &
    (col("suk_date_returned") < 20260201) &
    (col("buk_order_channel").isin('WORTENAPP','SITE'))
  )
)

df_prd = (
  df_table_prd
  .drop("dtm_created_at","dtm_updated_at")
  .filter(
    (col("suk_date_created") < 20260201) &
    (col("suk_date_delivered") < 20260201) &
    (col("suk_date_paid") < 20260201) &
    (col("suk_date_shipped") < 20260201) &
    (col("suk_date_cancelled") < 20260201) &
    (col("suk_date_returned") < 20260201)
  )
)

# COMMAND ----------

df_prd_except_dev = (
  df_prd.alias("a")
  .exceptAll(
    df_dev
  )
)

display(df_prd_except_dev)

# COMMAND ----------

df_dev_except_prd = (
  df_dev.alias("a")
  .exceptAll(
    df_prd
  )
)

display(df_dev_except_prd)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: All expected records in source tables are present in final table
# MAGIC This check needs to be manually adapted for each test

# COMMAND ----------

df_source = (
  spark.read.table("general_data_prd.curated.worten_customer_survey_responses")
  .filter(
    (col("SURVEYID").isin('SV_dmLggJjehlY0cDA','SV_8BRGQOP0Ksxlhc2','SV_eVxaoogMlsjHEp0')) &
    ~(col("VALUES.PROCESSTYPE").isin('D', 'SAT24')) &
    (col("LABELS.FINISHED") == 'True') &
    (col("VALUES.REQUESTNUMBER").isNotNull()) &
    (col("_internal_ingestion_timestamp_") <= '2026-02-28T15:03:12.954+00:00')
  )
  .select(
    col("metadata_stream_key").alias("cod_survey_response")
  )
)

# COMMAND ----------

df_all_surveys_in_target_table_that_are_not_in_source_table = (
  df_dev
  .exceptAll(
    df_source
  )
)
display(df_all_surveys_in_target_table_that_are_not_in_source_table)

# COMMAND ----------

df_all_surveys_in_source_table_that_are_not_in_target_table = (
  df_source
  .exceptAll(
    df_dev
  )
)
display(df_all_surveys_in_source_table_that_are_not_in_target_table)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: Describe history

# COMMAND ----------

qa.describe_history()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: Describe table

# COMMAND ----------

qa.describe_table()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check: flg_is_valid_now and dtm_valid_to

# COMMAND ----------

qa.records_validity_dim()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop HR table created for testing

# COMMAND ----------

if flg_is_HR_table == 'Yes':
  spark.sql(f"drop table {full_table_name_for_qa}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Other Manual Checks
# MAGIC - Does the table process deltas correctly?
# MAGIC - Do the values of the new records seem OK, both technically and make business sense? Do the new columns seem OK, both the source and the final columns?
# MAGIC - If the code has commented sections, was it analysed and updated too?
# MAGIC - Are all tables that are being read, in fact used?
# MAGIC - Are all notebook exits exiting with the right status?
# MAGIC - Is the code well commented?
# MAGIC - After reading the main source tables are we checking for 0 rows to exit the notebook?
# MAGIC - We are not removing duplicates in last dataframe before inserting with modelling framework? (We should not)
# MAGIC - Is the registerprocess function at the earliest point possible in the notebook code?
# MAGIC - Are the results of the tests documented in the respective Jira story/bug/defect?
# MAGIC - Are all joins with dimensions using dtm_scd_valid_from and dtm_scd_valid_to?
# MAGIC - Do all columns in the last dataframe have a name and alias?
# MAGIC - Do we have a coalesce in the all pk columns?
