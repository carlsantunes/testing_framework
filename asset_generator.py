# Databricks notebook source
# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

## Imports
import requests # Make API requests
from requests.auth import HTTPBasicAuth # Authentication for API requests
import json # Manage JSON content
from bs4 import BeautifulSoup # Manage HTML content
import re # regular expressions
import base64 # encode/decode string with notebook content to send/receive as payload for API request

# COMMAND ----------

from src.utils.logging_utils import log_setup_logic, log_info, log_warn, log_error, log_check_not_pass, log_check_pass
from src.utils.databricks_utils import ManageDatabricks
from src.utils.azure_devops_utils import ManageAzureDevOps
from src.utils.atlassian_utils import ManageAtlassian

# COMMAND ----------

# MAGIC %run "/Users/cvantunes@ext.worten.pt/Automation/Config File"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manage Logging

# COMMAND ----------

# MAGIC %md
# MAGIC #### Table design Confluence URL

# COMMAND ----------

# Link of table's confluence page (desenho)
dbutils.widgets.text("Confluence URL", "", "")
confluence_url = dbutils.widgets.get("Confluence URL")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize Databricks Management

# COMMAND ----------

md = ManageDatabricks(
  cfg.databricks_token,
  cfg.databricks_instance,
  cfg.atlassian_token,
  cfg.user_email
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize Azure DevOps Management

# COMMAND ----------

azDevOps = ManageAzureDevOps(
  cfg.azure_devops_token, 
  organization = "WORTEN", 
  project = "SINBA"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize Atlassian Management

# COMMAND ----------

confluence = ManageAtlassian(
  cfg.atlassian_token,
  cfg.user_email)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Notebook object

# COMMAND ----------

page_title, page_html_body = confluence.get_page(confluence_url)

# COMMAND ----------

nb = confluence.map_design_to_notebook(
  page_title = page_title, 
  page_body = page_html_body,
  confluence_url = confluence_url)

# COMMAND ----------

if "WDL" in page_title:
  repo_id = "DLM-Flows"
  repo_url = cfg.dlm_flows_repo_url
else:
  repo_id = "databricks-dev"
  repo_url = cfg.databricks_dev_repo_url

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a git folder with work branch

# COMMAND ----------

new_branch_name = azDevOps.create_working_branch(repo_id = repo_id, 
                               base_branch = "main",
                               feat_or_bug = "feature",
                               notebook_name = nb.notebook_name) 

# COMMAND ----------

databricks_repo_id, repo_workspace_path = md.create_git_folder(repo_url = repo_url,
                                          branch = "main",
                                          git_folder_suffix = nb.final_table.name)

# COMMAND ----------

# associate new working branch
md.associate_working_branch_git_folder(databricks_git_folder_id = databricks_repo_id, 
                                       new_branch = new_branch_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Generate New Notebook

# COMMAND ----------

# Define notebook location
if nb.notebook_type == 'DW CL':
  notebook_path = f"{repo_workspace_path}/Transformation_Notebooks/{'HR_Analytics/' if nb.final_table.flg_is_HR_table == True else ''}{nb.notebook_name}"
  md.create_notebook(notebook_content = nb.generate_notebook_code_string_format(), notebook_path = notebook_path)

elif nb.notebook_type == 'PRE-ING HR':
  notebook_path = f"{repo_workspace_path}/Transformation_Notebooks/HR_Analytics/Pre_Ingestion_Business/{nb.notebook_name}"
  md.create_notebook(notebook_content = nb.generate_notebook_code_string_format(), notebook_path = notebook_path)
  json_path = f"dbfs:/Volumes/people_dev/pre_ing/ingestion_hr/BUSINESS/config/{nb.json_name}"
  md.create_json(json_content = nb.generate_json_code_string_format(), json_path = notebook_path)

elif nb.notebook_type == 'WDL CSAS':
  sql_notebook_path = f"{repo_workspace_path}/dlm_flows/dlm_metadata/{nb.sql_notebook_name}"
  md.create_notebook(notebook_content = nb.generate_sql_code_string_format(), notebook_path = sql_notebook_path, language="SQL")
  notebook_path = f"{repo_workspace_path}/dlm_flows/{nb.notebook_name}"
  md.create_notebook(notebook_content = nb.generate_notebook_code_string_format(), notebook_path = notebook_path)

else:
  log_error(f"Table type {nb.notebook_type} not supported")
