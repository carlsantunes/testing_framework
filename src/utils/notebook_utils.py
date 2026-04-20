# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import base64 # encode/decode string with notebook content to send/receive as payload for API request
import time # Process times
import re # regular expressions
import json # Manage JSON content
from textwrap import dedent 


import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logging
# MAGIC Class to manage loggings. Has 5 methods: log_info, log_warn, log_error, log_check_pass, log_check_not_pass

# COMMAND ----------

# MAGIC %run "/Workspace/Users/cvantunes@ext.worten.pt/Automation/Utils/Logging Utils"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Classes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column class

# COMMAND ----------

## Column class (stores column metadata)
class Column:
  def __init__(self, name, data_type, description, primary_key = 'N/A', validation=[{'rule_description': 'N/A', 'error_message': 'N/A'}], flg_to_encrypt = False):
    self.name = name
    self.data_type = data_type
    self.description = description
    if flg_to_encrypt == True:
      if self.name in ('dtm_created_at', 'dtm_updated_at') or "suk_" in self.name:
        self.flg_is_encrypted = False
      elif flg_to_encrypt == True:
        self.flg_is_encrypted = True
    else:
      self.flg_is_encrypted = False
    self.validation = validation
    self.primary_key = primary_key
  	
  def __str__(self):
    return f"Column name: {self.name}, data_type: {self.data_type}, description: '{self.description}', primary_key: {self.primary_key}, flg_is_encrypted: {self.flg_is_encrypted}, validation: {self.validation}"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Table class

# COMMAND ----------

## Table class (stores table metadata)
class Table:
  def __init__(self, name, schema, catalog):
    self.name = name
    self.schema = schema
    self.catalog = catalog
    self.type_abv = self.load_table_type_abv()
    self.type = self.load_table_type()
    self.scd_type = None
  
  def load_table_type_abv(self):
    if self.name[:3] == 'agg':
      return 'agg'
    if self.name[:3] == 'fac':
      return 'fac'
    if self.name[:3] == 'dim':
      return 'dim'
    if self.name[:3] == 'cfg':
      return 'cfg'
    if self.name[:2] == 'vw':
      return 'vw'
    if self.name[:3] == 'rel':
      return 'rel'
    if self.name[:3] == 'wdl':
      return 'cfg'
    if self.schema == 'curated':
      return 'cur'
  
  def load_table_type(self):
    if self.name[:3] == 'agg':
      return 'aggregated'
    if self.name[:3] == 'fac':
      return 'factual'
    if self.name[:3] == 'dim':
      return 'dimension'
    if self.name[:3] == 'cfg':
      return 'configuration'
    if self.name[:3] == 'vw':
      return 'view'
    if self.name[:3] == 'rel':
      return 'relation'
    if self.name[:3] == 'wdl':
      return 'configuration'
    if self.schema == 'curated':
      return 'Curated'
  
  def load_scd_type(self):
    return
  
  def __str__(self):
    return f"\nTable Name: {self.name} \nSchema: {self.schema} \nCatalog: {self.catalog}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complex and Simple Table Subclasses

# COMMAND ----------

## SourceTable class (stores source tables metadata)
class SimpleTable(Table):
  def __init__(self, name, schema, catalog, condition):
    super().__init__(name, schema, catalog)
    self.filter = condition
  
  def __str__(self):
    return f"\nTable Name: {self.name} \nSchema: {self.schema} \nCatalog: {self.catalog} \nTable Type abv: {self.type_abv} \nTable Type: {self.type} \nFilter: {self.filter}"
  
## FinalTable class (stores final table metadata)
class ComplexTable(Table):
  def __init__(self, name, schema, catalog, description, column_list, pk='N/A', scd_type='N/A', columns_to_update='N/A'):
    super().__init__(name, schema, catalog)
    
    self.description = description
  
    self.flg_is_HR_table = True if self.catalog == 'people_dev' else False
    self.view_name = 'vw_' + self.name
    self.view_schema = 'business_views' if self.flg_is_HR_table == False else 'dw'
    self.view_catalog = catalog
    self.view_description = description.replace('Table', 'View', 1)

    self.primary_key = pk
    self.scd_type = scd_type
    self.columns_to_update = columns_to_update
    self.columns = column_list
  
  def __str__(self):
    columns_str = "\n".join([str(column) for column in self.columns])
    return f"\nTable Name: {self.name} \nSchema: {self.schema} \nCatalog: {self.catalog} \nTable Type abv: {self.type_abv} \nTable Type: {self.type} \nPrimary Key: {self.primary_key} \nColumns to update: {self.columns_to_update} \nTable Description: {self.description} \nView Description: {self.view_description} \nColumns: {columns_str}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook class

# COMMAND ----------

## Notebook Class (stores notebook metadata)
class Notebook:
  def __init__(self, notebook_name, squad, source_tables, final_table, notebook_type, confluence_url, user_email):
    log_info("Start notebook information loading...")

    self.table_types_list = ['agg', 'fac', 'dim', 'cfg', 'vw', 'rel', 'cur']
    self.notebook_type = notebook_type
    self.notebook_name = notebook_name
    self.squad = squad
    self.confluence_url = confluence_url
    self.source_tables = source_tables
    self.final_table = final_table
    self.user_email = user_email
    self.qa_notebook_name = f"QA Utils - {self.notebook_name}"
    self.qa_notebook_path = f"/Users/{user_email}/Testes QA/{self.qa_notebook_name}"
    
    log_info("Loading completed!")
     
  def __str__(self):
    source_tables_str = "\n".join([str(source_table) for source_table in self.source_tables])

    return (
            f"Notebook Type: {self.notebook_type}\n"
            f"Notebook Name: {self.notebook_name}\n"
            f"Squad: {self.squad}\n"
            f"Confluence URL: {self.confluence_url}\n"
            f"Source Tables: {source_tables_str}\n"
            f"Final Table: {self.final_table}\n"
            f"User Email: {self.user_email}\n"
            f"QA Notebook Name: {self.qa_notebook_name}\n"
            f"QA Notebook Path: {self.qa_notebook_path}\n"
          )


# COMMAND ----------

# MAGIC %md
# MAGIC ### PreIngNotebook Subclass

# COMMAND ----------

#
class PreIngNotebook(Notebook):
  def __init__(self, notebook_name, squad, source_tables, final_table, notebook_type, confluence_url, user_email):
    super().__init__(notebook_name, squad, source_tables, final_table, notebook_type, confluence_url, user_email)
    self.json_name = self.final_table.name.upper() + '.json'
  
  def _format_string_for_output(self, code_lines):
    return dedent("\n".join(code_lines))

  def create_code_header(self):
    return " "
  
  def create_code_utilities(self):
    code_lines = [
      "# MAGIC %md ## Invoke Utility Notebooks",
      "# COMMAND ----------",
      "# MAGIC %md",
      "# MAGIC Control_Utils_uc",
      "# COMMAND ----------",
      '# MAGIC %run "/Repos/dbw-dev-soi/Databricks-dev/Transformation_Notebooks/Control_Utils_uc"',
      "# COMMAND ----------",
      "# MAGIC %md",
      "# MAGIC decrypted_view_creation to decrypt a view",
      "# COMMAND ----------",
      '# MAGIC %run "/Repos/dbw-dev-soi/Databricks-dev/Transformation_Notebooks/HR_Analytics/Secure_Views/decrypted_view_creation"',
      "# COMMAND ----------",
      "# MAGIC %md",
      "# MAGIC hr_utils for encrypt and decrypt functions",
      "# COMMAND ----------",
      '# MAGIC %run "/Repos/dbw-dev-soi/Databricks-dev/Pre_Ingestion_Framework/hr_utils"'
    ]

    return self._format_string_for_output(code_lines)
  
  def create_code_define_param(self):
    code_lines = [
      "# COMMAND ----------",
      "# MAGIC %md",
      "# MAGIC ### Define Source and Target table",
      "# COMMAND ----------",
      "# Source Table",
      f"SOURCE_TABLE_NAME = '{self.source_tables[0].name}'",
      "SOURCE_TABLE = f'people_dev.curated.{SOURCE_TABLE_NAME}'",
      "",
      "# Target table name",
      f"TARGET_TABLE_NAME = '{self.final_table.name}'",
      "",
      "# Staging Table",
      "STAGING_TABLE = f'people_dev.stg.{TARGET_TABLE_NAME}'",
      "",
      "# Target Table",
      "TARGET_TABLE = f'people_dev.dw.{TARGET_TABLE_NAME}'",
      "TARGET_VIEW = f'people_dev.dw.vw_{TARGET_TABLE_NAME}'"
    ]
    return self._format_string_for_output(code_lines)
  
  def create_code_sequence(self):
    code_lines = [
      "# COMMAND ----------",
      "# MAGIC %md",
      "# MAGIC Get Sequence",
      "# COMMAND ----------",
      "LAST_SEQUENCE = spark.sql(f\"select seq_last from people_dev.control.config_table_properties where nme_process = '{SOURCE_TABLE_NAME}'\").first()[0]",
    ]
    
    return self._format_string_for_output(code_lines)
  
  def create_code_temp_view(self):
    code_lines = [
      "# COMMAND ----------",
      "# MAGIC %md",
      "# MAGIC ### Temp View Creation",
      "# COMMAND ----------",
      "def tempViewCreation(SOURCE_TABLE_NAME, LAST_SEQUENCE):",
      "  VALIDATION_TEMPVIEW = f'temp_view_{SOURCE_TABLE_NAME}_{LAST_SEQUENCE}'",
      '  filter_query = f"""',
      "    SELECT *",
      "    FROM {SOURCE_TABLE}",
      "    WHERE pre_ingested_sequence = '{LAST_SEQUENCE}'",
      '    """',
      "",        
      "  df = spark.sql(filter_query)",
      "  df = decrypt_any(df)",
      "  df.createOrReplaceTempView(VALIDATION_TEMPVIEW)",
      "  return df, VALIDATION_TEMPVIEW"
    ]
    return self._format_string_for_output(code_lines)
  
  def _val_cannot_be_null_or_empty(self, column_name, error_message, rule_description):
    code_lines = [
      f"    -- {rule_description}",
      "    SELECT RowNum,",
      f"        '{error_message}' AS err_msg",
      "    FROM {VALIDATION_TEMPVIEW}",
      f"    WHERE {column_name} IS NULL",
      f"       OR {column_name} = ''"
    ]
    return code_lines

  def _standard_validation_rule(self, column_name, error_message, rule_description):
    code_lines = [
      f"    -- {rule_description}",
      "    SELECT RowNum,",
      f"        '{error_message}' AS err_msg",
      "    FROM {VALIDATION_TEMPVIEW}",
      f"    WHERE {column_name}"
    ]
    return code_lines
  
  def create_code_validation(self):
    validation_queries = []
    for column in self.source_tables[0].columns:
      for val in column.validation:
        if val['rule_description'].upper() == "VALUE CANNOT BE NULL OR EMPTY":
          validation_queries.append(self._val_cannot_be_null_or_empty(column.name, val['error_message'], val['rule_description']))
        else:
          validation_queries.append(self._standard_validation_rule(column.name, val['error_message'], val['rule_description']))
        if not ((column == self.source_tables[0].columns[-1]) and (val == column.validation[-1])):
          validation_queries.append(["\n    UNION ALL\n"])

    validation_queries = [line for rule in validation_queries for line in rule]  

    code_lines = [
      "# COMMAND ----------",
      "# MAGIC %md",
      "# MAGIC ### Validate Logic",
      "# COMMAND ----------",
      "def validate(VALIDATION_TEMPVIEW):",
      "",
      '  validation_query = f"""',
    ] + validation_queries + [
      '  """',
      "",
      "  val_df = spark.sql(validation_query)",
      "  if val_df.count()>0:",
      "    # Exit with validation errors",
      "    val_success = False",
      '    val_message = "Invalid Data."',
      "    return val_success, val_message, val_df",
      "",
      "  # Exit with success",
      "  val_success = True",
      '  val_message = "Successful validation"',
      "  return val_success, val_message, val_df"
    ]
 
    return self._format_string_for_output(code_lines)
  
  def create_code_metadata_creation(self):
    column_lines_stg = []
    for cl in self.final_table.columns:
      if cl.name not in ('dtm_created_at', 'dtm_updated_at'):
        column_lines_stg.append([f"      {cl.name} {cl.data_type} comment '{cl.description}',"])

    column_lines_stg = [line for row in column_lines_stg for line in row]

    column_lines_dw = []
    for cl in self.final_table.columns:
      column_lines_dw.append([f"      {cl.name} {cl.data_type} comment '{cl.description}',"])

    column_lines_dw = [line for row in column_lines_dw for line in row]

    code_lines = [
      "# COMMAND ----------",
      "# MAGIC %md",
      "# MAGIC ### Metadata Creation",
      "# COMMAND ----------",
      "def create_metadata(TARGET_TABLE):",
      '  create_staging_table_query = f"""',
      "    CREATE TABLE IF NOT EXISTS {STAGING_TABLE} ("
    ] + column_lines_stg + [
      "    )",
      "    USING delta",
      f"    COMMENT '{self.final_table.description}'",
      '    """',
      "",      
      "  spark.sql(create_staging_table_query)",
      '  create_target_table_query = f"""',
      "    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} ("
    ] + column_lines_dw + [
      "  )",
      "    USING delta",
      f"    COMMENT '{self.final_table.description}'",
      '  """',
      "",        
      "  spark.sql(create_target_table_query)"
    ]

    return self._format_string_for_output(code_lines)
  
  
  def _get_value(self, obj, attr_name=None):
    if attr_name is None or isinstance(obj, (str, int, float, bool)):
        return obj
    else:
        return getattr(obj, attr_name, None)  # Returns None if attribute doesn't exist

  def _generate_list_of_table_columns(self, identation, list_to_iterate, string_exclude_last_iteration, main_string, item_attribute=""):
    new_list = []
    for i, item in enumerate(list_to_iterate):
      content = main_string.replace("item", self._get_value(item, item_attribute))
      if item != list_to_iterate[-1]:
        new_list.append([f"{identation}{content}{string_exclude_last_iteration}"])
      else:
        new_list.append([f"{identation}{content}"])

    return [line for row in new_list for line in row]

  def create_code_integration(self):

    pk = self._generate_list_of_table_columns("          ", self.final_table.primary_key, ",", '"item"')
    pk_merge = self._generate_list_of_table_columns("          ", self.final_table.primary_key, " AND", "target.item = source.item")
    update_columns = self._generate_list_of_table_columns("          ", self.final_table.columns_to_update, ",", "target.item = source.item")
    all_columns = self._generate_list_of_table_columns("          ", self.final_table.columns, ",", "item", "name")

    source_columns = []
    for cl in self.final_table.columns:
      if cl.name in ('dtm_created_at', 'dtm_updated_at'):
        source_columns.append("current_timestamp()")
      else:
        source_columns.append(cl.name)

    source_columns = self._generate_list_of_table_columns("          ", source_columns, ",", "source.item")

    code_lines = [
      "# COMMAND ----------",
      "# MAGIC %md",
      "# MAGIC ### Integrate Logic",
      "# COMMAND ----------"
      "def integrate(SOURCE_TABLE, TARGET_TABLE, LAST_SEQUENCE, df, VALIDATION_TEMPVIEW):",
      "    int_success = False",
      "    # Table dim_source_system",
      '    df_dim_source_system = spark.table("people_dev.dw.dim_source_system").select(',
      '        "buk_source_system", "suk_source_system"',
      "    )",
      "    # Transformations",
      "    df_final = (",
      '        df.alias("a")',
      "        .join(",
      '            df_dim_source_system.alias("b"),',
      '            col("b.buk_source_system") == lit(83),',
      '            "left"',
      "        )",
      "        .select("
      '            col("")'
      "        )",
      "    )",
      "",
      "    # Encryption",
      "    df = get_encrypt_df(",
      "        df_final,",
      "        scd_type=2,",
      "        not_encrypted_cols=[",
    ] + pk + [
      "        ]",
      "    )"
      "",
      "    # Insert into Staging Table",
      '    df.write.mode("overwrite").insertInto(STAGING_TABLE)',
      "    spark.sql(",
      '        f"""',
      "        MERGE INTO {TARGET_TABLE} AS target",
      "        USING {STAGING_TABLE} AS source",
      "        ON"
      ] + pk_merge + [
      "        WHEN MATCHED THEN UPDATE SET"
      ] + update_columns + [
      "        WHEN NOT MATCHED THEN",
      "        INSERT ("
      ] + all_columns + [
      "        )",
      "        VALUES ("
      ] + source_columns + [
      "        )",
      '    """',
      "    )",
      "",
      "    # Signal integration success",
      "    int_success = True",
      '    int_message = "Successful integration"',
      "    return int_success, int_message",
    ]

    return self._format_string_for_output(code_lines)
  
  def create_code_view(self):
    columns = []
    for i, cl in enumerate(self.final_table.columns):
      columns.append([f"{{'col_order':	{i+1}, 'col_name':'{cl.name}', 'type':'{cl.data_type}', 'encrypted':{cl.flg_is_encrypted}}},"])

    columns = [line for row in columns for line in row]

    code_lines = [
      "# COMMAND ----------",
      "# MAGIC %md",
      "# MAGIC ### View Creation",
      "# COMMAND ----------",
      "def createFinalView(FINAL_TABLE, TARGET_VIEW):",
      "  # Set parameters for target view creation function ",
      "  viewName = TARGET_VIEW",
      "  sourceTableNameOrStatement = FINAL_TABLE",
      "  colCastToDataType =["
    ] + columns + [
      "  ]",
      f"  viewComment='{self.final_table.description}'",
        
      "  # Create target view",
      "  udf_create_target_view_hr(viewName=viewName, sourceTableNameOrStatement=sourceTableNameOrStatement, colCastToDataType=colCastToDataType, viewComment=viewComment, recreateView=True)",
    ]
  

    return self._format_string_for_output(code_lines)
  
  def create_code_execution(self):
    code_lines = [
      "# COMMAND ----------",
      "# MAGIC %md",
      "# MAGIC ### Execution of Validation, Metadate creation and Integration",
      "# COMMAND ----------",
      "df, VALIDATION_TEMPVIEW= tempViewCreation(SOURCE_TABLE_NAME, LAST_SEQUENCE)",
      "val_success, val_message, val_df = validate(VALIDATION_TEMPVIEW)",
      "if val_success:",
      "  create_metadata(TARGET_TABLE)",
      "  integrate(SOURCE_TABLE, TARGET_TABLE, LAST_SEQUENCE, df, VALIDATION_TEMPVIEW)",
      "  createFinalView(TARGET_TABLE, TARGET_VIEW)",
      "  dbutils.notebook.exit('Success!')",
      "else:",
      "  display(val_df)",
      '  raise Exception("Errors detected in Validations!")'
    ]
    
    return self._format_string_for_output(code_lines)

  def generate_notebook_code_string_format(self):
    return "\n".join([
      self.create_code_header(),
      self.create_code_utilities(),
      self.create_code_define_param(),
      self.create_code_sequence(),
      self.create_code_temp_view(),
      self.create_code_validation(),
      self.create_code_metadata_creation(),
      self.create_code_integration(),
      self.create_code_view(),
      self.create_code_execution()
    ])

  def generate_json_code_string_format(self):
  
    columns = []
    for i, cl in enumerate(self.source_tables[0].columns):
      columns.append(
        {
          "column_number": f"{i+2}",
          "column_name": f"{cl.name}",
          "type": "string",
          "encrypt": "2",
          "PK": f"{cl.primary_key}",
          "comment": f"{cl.description}",
          "date_format": "",
          "column_alias": f"{cl.name}",
          "column_generated": "False"
        }
      )

    table_name = self.source_tables[0].name.upper()

    json_lines = {
      "process_name": f"{table_name}",
      "metadata": {
        "filename": [
          {
            "file": f"{table_name}_#seq.csv.gpg",
            "table_name": f"{table_name}",
            "origin": "BUSINESS",
            "columns": [
                {
                  "column_number": "1",
                  "column_name": "RowNum",
                  "type": "int",
                  "encrypt": "0",
                  "PK": "False",
                  "comment": "Row Num",
                  "date_format": "",
                  "column_alias": "",
                  "column_generated": "False"
                }
            ],
            "where_clause": "",
            "purge": "False",
            "anonymization": "False",
            "description": f"{self.source_tables[0].description}",
           "separator": ";",
            "file_path": "/ingestion/BUSINESS",
            "destination_file_path": "",
            "final_table_mode": "append",
            "final_table_format": "delta",
            "window": "0",
            "window_value": "",
            "keep": "0",
            "partition_by": "",
            "escapechar": "\\",
            "window_period": "Y"
          }
        ]
      }
    }

    json_lines["metadata"]["filename"][0]["columns"].extend(columns)

    return json.dumps(json_lines, indent=4) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### DWCLNotebok Subclass

# COMMAND ----------

#
class DWCLNotebook(Notebook):
  def __init__(self, notebook_name, squad, source_tables, final_table, notebook_type, confluence_url, user_email):
    super().__init__(notebook_name, squad, source_tables, final_table, notebook_type, confluence_url, user_email)

  # Notebook's header
  def create_code_header(self):
    list_scr_tables_str = {}
    for table_type_abv in self.table_types_list:
      list_scr_tables_str[table_type_abv] = "; ".join([f"{tb.catalog}.{tb.schema}.{tb.name}" for tb in self.source_tables if tb.type_abv == table_type_abv])
      if list_scr_tables_str[table_type_abv] == "":
        list_scr_tables_str[table_type_abv] = "N/A"

    return f"""# Databricks notebook source 
    # MAGIC %md
    # MAGIC ## Notebook Objectives:
    # MAGIC
    # MAGIC * **Goal:** Consolidation of {self.final_table.type} table {self.final_table.catalog}.{self.final_table.schema}.{self.final_table.name}
    # MAGIC
    # MAGIC * **SCD Type:** {self.final_table.scd_type}
    # MAGIC
    # MAGIC * **Source Tables**:
    # MAGIC
    # MAGIC   * **Aggregate Tables**: {list_scr_tables_str['agg']}
    # MAGIC
    # MAGIC   * **Factual Tables**: {list_scr_tables_str['fac']}
    # MAGIC
    # MAGIC   * **Dimension Tables**: {list_scr_tables_str['dim']}
    # MAGIC
    # MAGIC   * **Configuration Tables**: {list_scr_tables_str['cfg']}
    # MAGIC
    # MAGIC   * **Curated Tables**: {list_scr_tables_str['cur']}
    # MAGIC
    # MAGIC - **Confluence link for table consolidation:** {self.confluence_url}
    # MAGIC
    # MAGIC - **Related Views**: {self.final_table.view_catalog}.{self.final_table.view_schema}.{self.final_table.view_name}
    # MAGIC
    # MAGIC - **Other Notes**: N/A
    """

  def create_code_utilities(self):
    general_utility = f"""# COMMAND ----------

    # MAGIC %md ## Invoke Utility Notebook

    # COMMAND ----------

    # MAGIC %run "/Repos/dbw-dev-soi/Databricks-dev/Transformation_Notebooks/Control_Utils_uc"
    """

    hr_utility = f"""# COMMAND ----------

    # MAGIC %run "/Repos/dbw-dev-soi/Databricks-dev/Transformation_Notebooks/HR_Analytics/Secure_Views/decrypted_view_creation"

    # COMMAND ----------

    # MAGIC %run "/Repos/dbw-dev-soi/Databricks-dev/Pre_Ingestion_Framework/hr_utils"
    """

    return general_utility + hr_utility if self.final_table.flg_is_HR_table else general_utility

  def create_code_func_declaration(self):
    return f"""# COMMAND ----------

    # MAGIC %md
    # MAGIC ## In-Built Function Declaration
    # MAGIC This command is not being used
    """

  def create_code_widgets(self):
    return f"""# COMMAND ----------

    # MAGIC %md ## Widgets Declaration
    # MAGIC This command is not being used
    """

  def create_code_paths(self):
    return f"""# COMMAND ----------
  
    # MAGIC %md ## Path Declaration
    """

  def create_code_start_control(self):
    return dedent(f"""\
    # COMMAND ----------
  
    # MAGIC %md #### Initialize control table process

    # COMMAND ----------

    # Parameter should be fill with the name of the DW table
    processName = "{self.notebook_name}"

    # Register execution and obtain key assigned to process
    processKey = registerProcessExecution(processName)
    """)

  def create_code_table_creation(self):
    columns_statement = ",\n# MAGIC   ".join(f"{cl.name} STRING COMMENT '{cl.description}'" for cl in self.final_table.columns)

    return f"""# COMMAND ----------

    # MAGIC %md #### Table Creation

    # COMMAND ----------

    # MAGIC %sql
    # MAGIC CREATE TABLE IF NOT EXISTS {self.final_table.catalog}.{self.final_table.schema}.{self.final_table.name} (
    # MAGIC   {columns_statement}
    # MAGIC )
    # MAGIC USING DELTA
    # MAGIC COMMENT '{self.final_table.description}'
    """

  def create_code_table_tags(self):
    return dedent(f"""\
    # COMMAND ----------

    # MAGIC %md
    # MAGIC ##### Validation of Owner and Documentation Tags

    # COMMAND ----------

    # Get table current tags
    df_tags = spark.sql(
      f\"\"\"
      SELECT *
      FROM system.information_schema.table_tags
      WHERE catalog_name = '{self.final_table.catalog}'
        AND schema_name = '{self.final_table.schema}'
        AND table_name = '{self.final_table.name}'
      \"\"\"
    )

    # If owner and documentation tags are not present, then create them
    if df_tags.filter(df_tags["tag_name"] == 'owner').limit(1).count() == 0:
      squad = "{self.squad}"
      spark.sql(
      f\"\"\"ALTER TABLE {self.final_table.catalog}.{self.final_table.schema}.{self.final_table.name}
      SET TAGS ('owner' = '{{squad}}')\"\"\")

    if df_tags.filter(df_tags["tag_name"] == 'documentation').limit(1).count() == 0:
      documentation = "{self.confluence_url}"
      spark.sql(
      f\"\"\"ALTER TABLE {self.final_table.catalog}.{self.final_table.schema}.{self.final_table.name}
      SET TAGS ('documentation' = '{{documentation}}')\"\"\")
    """)

  def create_code_get_dependencies(self):
    getDaysProcessedByDepedency = []
    unions = []
    tables_dep = []

    # create a list with tables
    for src in self.source_tables:
      for table_type in ['agg', 'fac', 'dim']:
        if table_type in src.type_abv:
          tables_dep.append(src)

    for src in tables_dep:    
      getDaysProcessedByDepedency.extend([
          f"# Get days processed by trf_{src.name} since last execution",
          f"df_{src.name}_days_to_process = getDaysProcessedByDepedency(",
          f'  "trf_{src.name}", strLastRunStartDate',
          f")"
      ])
    
      if src == tables_dep[0]:
        unions.extend([
            f"df_records_to_process = (",
            f"  df_{src.name}_days_to_process"
        ])
      else:
        unions.extend([
            f"  .union(",
            f"    df_{src.name}_days_to_process",
            f"  )"
        ])

      if src == tables_dep[-1]:
          unions.append(")")

    getDaysProcessedByDepedency = dedent("\n".join(getDaysProcessedByDepedency))
    unions = dedent("\n".join(unions))
    
    final_code_lines = [
      "# COMMAND ----------",
      "",
      "# MAGIC %md",
      "# MAGIC ##### Get Days To Process From Dependencies",
      "",
      "# COMMAND ----------",
      "",
      *getDaysProcessedByDepedency.splitlines(),
      "",
      *unions.splitlines(),
      "",
      "df_records_to_process = df_records_to_process.dropDuplicates()",
      "",
      "# Check if there are any records to process",
      "if df_records_to_process.count() == 0:",
      " exitNotebookOuput(",
      "   processKey,",
      "   processName,",
      '   "ALERT",',
      '   "No rows processed",',
      '   rowsRead="0",',
      '   rowsInserted="0",',
      '   rowsUpdated="0",',
      '   rowsDeleted="0",',
      " )",
      "",
      "# Obtain the list of days and months to read from source tables",
      "lst_days_to_read, lst_months_to_read = getDaysMonthsToRead(df_records_to_process)",
      ""
    ]

    return dedent("\n".join(final_code_lines))


  def create_code_reads(self):
    all_reads = ""
    for src_tb in self.source_tables:
      single_read = dedent(f"""\
      # Read data from {src_tb.catalog}.{src_tb.schema}.{src_tb.name}
      df_{src_tb.name} = (
        spark.read.table("{src_tb.catalog}.{src_tb.schema}.{src_tb.name}")
      """)
      
      if src_tb.filter != '':
        single_read += f"\n  .filter({src_tb.filter})"
      
      single_read += dedent(f"""\
      )
      df_{src_tb.name} = decrypt_any(df_{src_tb.name})\n
      """) if self.final_table.flg_is_HR_table == True else ""

      all_reads += single_read 

    return f"""\
    # COMMAND ----------

    # MAGIC %md ## Read From Source Tables

    # COMMAND ----------

    {all_reads}
    """

  def create_code_business_logic(self):
    return dedent(f"""\
    # COMMAND ----------

    # MAGIC %md
    # MAGIC ## Business Logic and Transformations

    # COMMAND ----------
    """)


  def create_code_exit(self):
    return dedent(f"""\
    # COMMAND ----------

    # Exit notebook with Warning if df_{self.final_table.name} is empty
    rows_read = df_final.count()

    if rows_read == 0:
      exitNotebookOuput(processKey, process_name, status='WARNING', errorMessage='No rows read')
    """)

  def create_code_encryption(self):
    return dedent(f"""\
    # COMMAND ----------

    # MAGIC %md #### Encrypt data

    # COMMAND ----------

    list_type_1_cols = ['suk_date', 'buk_employee_office', 'buk_office']
    df_final_encrypted = get_encrypt_df_facs(df_final, type_1_cols = list_type_1_cols)
    """)

  def create_code_stg(self):
    return dedent(f"""\
    # COMMAND ----------

    # MAGIC %md
    # MAGIC ### Overwrite Staging table

    # COMMAND ----------

    df_final_encrypted.write.format("delta").mode("overwrite").option("overwriteSchema", "True").saveAsTable("{self.final_table.catalog}.stg.{self.final_table.name}")
    """)

  def create_code_dw(self):
    using = ""
    for key in self.final_table.primary_key:
      using += 'target.' + key + ' = source.' + key
      if key != self.final_table.primary_key[-1]:
        using += ' AND\n  '

    update = ""
    for column_name in self.final_table.columns_to_update:
      if column_name in ('dtm_updated_at'):
        update += 'target.' + column_name + ' = current_timestamp()'
      else:
        update += 'target.' + column_name + ' = source.' + column_name
      if column_name != self.final_table.columns_to_update[-1]:
        update += ',\n  '

    insert = ""
    values = ""
    for column_name in self.final_table.columns:
      insert += column_name.name
      if column_name.name in ('dtm_created_at', 'dtm_updated_at'):
        values += 'current_timestamp()'
      else:
        values += 'source.' + column_name.name
      if column_name != self.final_table.columns[-1]:
        insert += ',\n  '
        values += ',\n  '


    return f"""\
    # COMMAND ----------

    # MAGIC %md
    # MAGIC ### Write into Data Lake

    # COMMAND ----------

    # MAGIC %sql
    # MAGIC MERGE INTO {self.final_table.catalog}.{self.final_table.schema}.{self.final_table.name} AS target
    # MAGIC USING {self.final_table.catalog}.stg.{self.final_table.name} AS source
    # MAGIC ON {using}
    # MAGIC WHEN MATCHED THEN
    # MAGIC UPDATE
    # MAGIC SET
    # MAGIC   {update}
    # MAGIC WHEN NOT MATCHED 
    # MAGIC THEN INSERT
    # MAGIC (
    # MAGIC   {insert}
    # MAGIC )
    # MAGIC VALUES
    # MAGIC (
    # MAGIC   {values}
    # MAGIC )
    """

  def create_code_view(self):
    colcast = ""
    for i, col in enumerate(self.final_table.columns):
      colcast += "{'col_order':" + str(i+1) + ", 'col_name': '" + col.name + "', 'type': '" + col.data_type + "', 'encrypted': " + str(col.flg_is_encrypted) + "}"
      if col != self.final_table.columns[-1]:
        colcast += ",\n "

    return dedent(f"""\
    # COMMAND ----------

    # MAGIC %md
    # MAGIC ## View Creation

    # COMMAND ----------

    # Set parameters for target view creation function
    viewName ='{self.final_table.catalog}.dw.vw_{self.final_table.name}'
    sourceTableNameOrStatement = '{self.final_table.catalog}.{self.final_table.schema}.{self.final_table.name}'

    colCastToDataType = [
      {colcast}
    ]
    viewComment = '{self.final_table.view_description}'

    # Create target view vw_{self.final_table.name}
    udf_create_target_view_hr(
      viewName=viewName,
      sourceTableNameOrStatement=sourceTableNameOrStatement,
      colCastToDataType=colCastToDataType,
      viewComment=viewComment,
      recreateView=True
    )
    """)

  def create_code_end_control(self):
    return dedent(f"""\
    # COMMAND ----------

    # MAGIC %md ## End Control Table Process
    
    # COMMAND ----------

    # Obtain Statistics
    table_name = "{self.final_table.catalog}.{self.final_table.schema}.{self.final_table.name}"
    statistics = get_statistics(table_name)

    # Obtain list of days processed
    listDaysProcessed_sql = (
      decrypt_any(spark.read.table("{self.final_table.catalog}.stg.{self.final_table.name}"))
      .select(
        col("suk_date")
      )
      .distinct()
      .orderBy(
        col("suk_date")
      )
    )

    listDaysProcessedStr = convertColumnJSONArray(listDaysProcessed_sql, "suk_date", True, "days")

    ## leave workbook and provide output statistics to pipeline and log
    exitNotebookOuput(processKey,
                      processName,
                      "OK",
                      rowsRead = statistics["rows_read"],
                      rowsInserted = statistics["rows_inserted"],
                      rowsUpdated = statistics["rows_updated"],
                      rowsDeleted = statistics["rows_deleted"],
                      listDaysProcessed=listDaysProcessedStr
                      )
    """)

  def generate_notebook_code_string_format(self):
    return "".join([
      self.create_code_header(),
      self.create_code_utilities(),
      self.create_code_func_declaration(),
      self.create_code_widgets(),
      self.create_code_paths(),
      self.create_code_start_control(),
      self.create_code_table_creation(),
      self.create_code_table_tags(),
      self.create_code_get_dependencies(),
      self.create_code_reads(),
      self.create_code_business_logic(),
      self.create_code_exit(),
      self.create_code_encryption(),
      self.create_code_stg(),
      self.create_code_dw(),
      self.create_code_view(),
      self.create_code_end_control()
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ### WDLNotebook Subclass

# COMMAND ----------

#
class WDLNotebook(Notebook):
  def __init__(self, notebook_name, sql_notebook_name, squad, source_tables, final_table, notebook_type, confluence_url, user_email, flow_owner, example_csv_row):
    super().__init__(notebook_name, squad, source_tables, final_table, notebook_type, confluence_url, user_email)
    self.sql_notebook_name = sql_notebook_name
    self.flow_owner = flow_owner
    self.example_csv_row = example_csv_row
    self.flow_key = self.final_table.name


  def create_code_sql_script(self):

    create_table = f"CREATE TABLE dlmdata.{self.final_table.name} (\n"
    create_table += ",\n".join(f"        {cl.name}     VARCHAR(250)" for cl in self.source_tables[0].columns)
    create_table += "\n      );"

    return dedent(f"""\
      INSERT INTO dlm.dlm_load_flow (
        flow_key,
        flow_type,
        flow_description,
        procedure_validation,
        parameter_1,
        parameter_2,
        parameter_3,
        parameter_4,
        parameter_5,
        parameter_6,
        parameter_7,
        parameter_8,
        parameter_9,
        parameter_10,
        parameter_11,
        parameter_12,
        parameter_13,
        parameter_14,
        parameter_15,
        parameter_16,
        parameter_17,
        parameter_18,
        parameter_19,
        parameter_20,
        destination_schema,
        destination_table,
        system_integration,
        system_flow,
        flow_verification_query,
        flow_filename,
        flow_file_contents,
        server_name,
        appendable_data,
        user_confirm_integr,
        output_mechanism,
        flow_destination,
        procedure_integration,
        error_verif_table,
        error_verif_field,
        error_verif_suc_msg,
        verif_filter_rows,
        suc_verif_table,
        suc_verif_field,
        ct_table_expression,
        purge_hist_files_days,
        output_timeout,
        sftp,
        scp,
        ftrs_host,
        ftrs_port,
        ftrs_username,
        ftrs_password,
        ftrs_dir,
        exp_separator,
        exp_file_extension,
        inp_separator,
        inp_file_extension,
        portion,
        multiple_loads_type,
        file_include_header,
        portion_header,
        portion_footer,
        portion_name_expression,
        current_load_number,
        current_load_days,
        error_verif_order,
        users_only_see_own_loads,
        error_verif_def_val,
        flow_validation_rules_link,
        azcopy,
        run_pipeline,
        pipeline_name,
        owner
      )
      VALUES (
        '{self.flow_key}',                                 -- flow_key
        '2',                                                                    -- flow_type
        '{self.final_table.description}',      -- flow_description
        'DLMDATA.VALIDATE_DUMMY',                                               -- procedure_validation
        NULL,                                                                   -- parameter_1
        NULL,                                                                   -- parameter_2
        NULL,                                                                   -- parameter_3
        NULL,                                                                   -- parameter_4
        NULL,                                                                   -- parameter_5
        NULL,                                                                   -- parameter_6
        NULL,                                                                   -- parameter_7
        NULL,                                                                   -- parameter_8
        NULL,                                                                   -- parameter_9
        NULL,                                                                   -- parameter_10
        NULL,                                                                   -- parameter_11
        NULL,                                                                   -- parameter_12
        NULL,                                                                   -- parameter_13
        NULL,                                                                   -- parameter_14
        NULL,                                                                   -- parameter_15
        NULL,                                                                   -- parameter_16
        NULL,                                                                   -- parameter_17
        NULL,                                                                   -- parameter_18
        NULL,                                                                   -- parameter_19
        NULL,                                                                   -- parameter_20
        'DLMDATA',                                                              -- destination_schema
        '{self.final_table.name}',                                 -- destination_table
        '1',                                                                    -- system_integration
        NULL,                                                                   -- system_flow
        NULL,                                                                   -- flow_verification_query
        '{self.flow_key}',                                          -- flow_filename
        '{self.example_csv_row}',  -- flow_file_contents
        'DATABRICKS-DW',                                                        -- server_name
        'TRUNCATE',                                                             -- appendable_data
        'N',                                                                    -- user_confirm_integr
        'API',                                                                  -- output_mechanism
        NULL,                                                                   -- flow_destination
        NULL,                                                                   -- procedure_integration
        NULL,                                                                   -- error_verif_table
        NULL,                                                                   -- error_verif_field
        NULL,                                                                   -- error_verif_suc_msg
        NULL,                                                                   -- verif_filter_rows
        NULL,                                                                   -- suc_verif_table
        NULL,                                                                   -- suc_verif_field
        NULL,                                                                   -- ct_table_expression
        15,                                                                     -- purge_hist_files_days
        0,                                                                      -- output_timeout
        0,                                                                      -- sftp
        0,                                                                      -- scp
        NULL,                                                                   -- ftrs_host
        NULL,                                                                   -- ftrs_port
        NULL,                                                                   -- ftrs_username
        NULL,                                                                   -- ftrs_password
        '/WDL/sls/{self.final_table.name.upper()}',                        -- ftrs_dir
        ';',                                                                    -- exp_separator
        '.CSV',                                                                 -- exp_file_extension
        ';',                                                                    -- inp_separator
        '.CSV',                                                                 -- inp_file_extension
        0,                                                                      -- portion
        0,                                                                      -- multiple_loads_type
        1,                                                                      -- file_include_header
        NULL,                                                                   -- portion_header
        NULL,                                                                   -- portion_footer
        NULL,                                                                   -- portion_name_expression
        0,                                                                      -- current_load_number
        0,                                                                      -- current_load_days
        NULL,                                                                   -- error_verif_order
        0,                                                                      -- users_only_see_own_loads
        NULL,                                                                   -- error_verif_def_val
        '{self.confluence_url}',    -- flow_validation_rules_link
        1,                                                                      -- azcopy
        1,                                                                      -- run_pipeline
        'run_dlm_engine',                                                       -- pipeline_name
        '{self.flow_owner}'                                                 -- owner
      )
      ON CONFLICT (flow_key) DO UPDATE
        SET flow_type              = EXCLUDED.flow_type,
          flow_description           = EXCLUDED.flow_description,
          procedure_validation       = EXCLUDED.procedure_validation,
          parameter_1                = EXCLUDED.parameter_1,
          parameter_2                = EXCLUDED.parameter_2,
          parameter_3                = EXCLUDED.parameter_3,
          parameter_4                = EXCLUDED.parameter_4,
          parameter_5                = EXCLUDED.parameter_5,
          parameter_6                = EXCLUDED.parameter_6,
          parameter_7                = EXCLUDED.parameter_7,
          parameter_8                = EXCLUDED.parameter_8,
          parameter_9                = EXCLUDED.parameter_9,
          parameter_10               = EXCLUDED.parameter_10,
          parameter_11               = EXCLUDED.parameter_11,
          parameter_12               = EXCLUDED.parameter_12,
          parameter_13               = EXCLUDED.parameter_13,
          parameter_14               = EXCLUDED.parameter_14,
          parameter_15               = EXCLUDED.parameter_15,
          parameter_16               = EXCLUDED.parameter_16,
          parameter_17               = EXCLUDED.parameter_17,
          parameter_18               = EXCLUDED.parameter_18,
          parameter_19               = EXCLUDED.parameter_19,
          parameter_20               = EXCLUDED.parameter_20,
          destination_schema         = EXCLUDED.destination_schema,
          destination_table          = EXCLUDED.destination_table,
          system_integration         = EXCLUDED.system_integration,
          system_flow                = EXCLUDED.system_flow,
          flow_verification_query    = EXCLUDED.flow_verification_query,
          flow_filename              = EXCLUDED.flow_filename,
          flow_file_contents         = EXCLUDED.flow_file_contents,
          server_name                = EXCLUDED.server_name,
          appendable_data            = EXCLUDED.appendable_data,
          user_confirm_integr        = EXCLUDED.user_confirm_integr,
          output_mechanism           = EXCLUDED.output_mechanism,
          flow_destination           = EXCLUDED.flow_destination,
          procedure_integration      = EXCLUDED.procedure_integration,
          error_verif_table          = EXCLUDED.error_verif_table,
          error_verif_field          = EXCLUDED.error_verif_field,
          error_verif_suc_msg        = EXCLUDED.error_verif_suc_msg,
          verif_filter_rows          = EXCLUDED.verif_filter_rows,
          suc_verif_table            = EXCLUDED.suc_verif_table,
          suc_verif_field            = EXCLUDED.suc_verif_field,
          ct_table_expression        = EXCLUDED.ct_table_expression,
          purge_hist_files_days      = EXCLUDED.purge_hist_files_days,
          output_timeout             = EXCLUDED.output_timeout,
          sftp                       = EXCLUDED.sftp,
          scp                        = EXCLUDED.scp,
          ftrs_host                  = EXCLUDED.ftrs_host,
          ftrs_port                  = EXCLUDED.ftrs_port,
          ftrs_username              = EXCLUDED.ftrs_username,
          ftrs_password              = EXCLUDED.ftrs_password,
          ftrs_dir                   = EXCLUDED.ftrs_dir,
          exp_separator              = EXCLUDED.exp_separator,
          exp_file_extension         = EXCLUDED.exp_file_extension,
          inp_separator              = EXCLUDED.inp_separator,
          inp_file_extension         = EXCLUDED.inp_file_extension,
          portion                    = EXCLUDED.portion,
          multiple_loads_type        = EXCLUDED.multiple_loads_type,
          file_include_header        = EXCLUDED.file_include_header,
          portion_header             = EXCLUDED.portion_header,
          portion_footer             = EXCLUDED.portion_footer,
          portion_name_expression    = EXCLUDED.portion_name_expression,
          current_load_number        = EXCLUDED.current_load_number,
          current_load_days          = EXCLUDED.current_load_days,
          error_verif_order          = EXCLUDED.error_verif_order,
          users_only_see_own_loads   = EXCLUDED.users_only_see_own_loads,
          error_verif_def_val        = EXCLUDED.error_verif_def_val,
          flow_validation_rules_link = EXCLUDED.flow_validation_rules_link,
          azcopy                     = EXCLUDED.azcopy,
          run_pipeline               = EXCLUDED.run_pipeline,
          pipeline_name              = EXCLUDED.pipeline_name,
          owner                      = EXCLUDED.owner;

      DROP TABLE IF EXISTS dlmdata.{self.final_table.name};

      {create_table}
      """)

  def generate_sql_code_string_format(self):
    return "".join([
      self.create_code_sql_script()
    ])


  def create_code_init(self):
    return dedent(f"""\
      \"\"\"
      Functions to validate and integrate a load of
      {self.flow_key} flow.
      \"\"\"

      import logging

      from dlm_engine._globals import ENV, spark
      from dlm_flows.utils import is_empty

      logger = logging.getLogger(__name__)

      # Flow ID
      FLOW_KEY = '{self.flow_key}'

      # Table Name
      TABLE_NAME = FLOW_KEY.lower()

      # Source (raw) table
      SOURCE_TABLE = f'general_data_{{ENV.lower()}}.raw.wdl_{{FLOW_KEY.upper()}}'
      SOURCE_LOCATION = f'abfss://ingestion@st{{ENV.lower()}}datalake001.dfs.core.windows.net/WDL/sls/{{FLOW_KEY.upper()}}'

      # Curated (curated) Table
      CURATED_TABLE = f'general_data_{{ENV.lower()}}.curated.wdl_{{FLOW_KEY.lower()}}'

      # Target (dw) table
      TARGET_TABLE = f'sales_{{ENV.lower()}}.dw.{{FLOW_KEY.lower()}}'
      """)

  def create_code_validation(self):
    return dedent(f'''\
      def validate(load_key, flow_key, *args):
      """Validate a load for this flow.

      Queries the load's data from source table to determine its validity.

      Args:
          load_key (str):
              Identifier of the load.
          flow_key (str):
              Identifier of the flow.
          args (list[str], optional):
              List of additional parameters configured for this flow.
              The user who submitted the load is always included.

      Returns:
          val_success (bool):
              Whether the load is valid (True or False).
          val_message (str):
              Exit message of the validation process.
          val_df (pyspark.sql.dataframe.DataFrame, optional):
              Spark dataframe with row-level error/non-error messages.
              The following structures are accepted:
                  - DataFrame[Row_ID: string, Message: string]
                  - DataFrame[Row_ID: string, Message: string, Is_Error: boolean]
              By default, all records are assumed to be error messages.
              The columns' order (index) is fixed; their name may be any.

      """
      # Ensure exclusive use for intended flow
      assert flow_key.upper() == FLOW_KEY.upper()

      # Refresh source table
      spark.sql(f"REFRESH TABLE {{SOURCE_TABLE}}")

      # Create temporary view with records of current load to validate
      LOAD_RECORDS_TEMPVIEW = f"LOAD{{load_key}}_RECORDS_TEMPVIEW"
      filter_query = f"""
          SELECT *
            FROM {{SOURCE_TABLE}}
          WHERE load_key = '{{load_key}}'
          """
      logger.info("Creating temporary view '%s' for LOAD=%s, FLOW=%s: %s",
                  LOAD_RECORDS_TEMPVIEW, load_key, flow_key, filter_query)
      load_records_df = spark.sql(filter_query)
      load_records_df.createOrReplaceTempView(LOAD_RECORDS_TEMPVIEW)
      spark.catalog.cacheTable(LOAD_RECORDS_TEMPVIEW)

      # Guard against no data
      if is_empty(load_records_df):
          spark.catalog.dropTempView(LOAD_RECORDS_TEMPVIEW)
          return False, f"No data found for load {{load_key}}."

      # Validation query
      
    # Validation query
      validation_query = f"""
        SELECT rowid
            , 'Duplicate values (for the same date, location and info type have different values).' AS err_msg
        FROM {{LOAD_RECORDS_TEMPVIEW}}

        UNION ALL

      """

      logger.info("Partial validation query for LOAD=%s, FLOW=%s: %s",
                    load_key, flow_key, validation_query)

      # Validation dataframe
      val_df = spark.sql(validation_query)
        
      # Drop temporary view
      spark.catalog.dropTempView(LOAD_RECORDS_TEMPVIEW)

      # Evaluate global validity
      if not is_empty(val_df):
                # Exit with validation errors
                val_success = False
                val_message = "Invalid Data. Please verify WDL grid."
                return val_success, val_message, val_df
              
      # Exit with success
      val_success = True
      val_message = "Successful validation"
      return val_success, val_message

      ''')
  
  def create_code_integration(self):

    if self.final_table.columns == self.final_table.columns_to_update:
      columns = ",\n      ".join(f"{cl.name}" for cl in self.final_table.columns)

      curated_query = f"""INSERT OVERWRITE TABLE {{CURATED_TABLE}}
      SELECT {columns}
      FROM {{SOURCE_TABLE}}
      WHERE load_key = '{{load_key}}'
      """

      dw_query = f"""INSERT OVERWRITE TABLE {{TARGET_TABLE}}
      SELECT {columns}
           , CURRENT_TIMESTAMP() AS dtm_created_at
           , CURRENT_TIMESTAMP() AS dtm_updated_at
      FROM {{CURATED_TABLE}}
      """
    else:
      curated_query = ""
      dw_query = ""


    return dedent(f'''\
      def integrate(load_key, flow_key, *args):
      """Integrate a load for this flow.

      Inserts the load's data into the curated and target tables.

      Args:
          load_key (str):
              Identifier of the load.
          flow_key (str):
              Identifier of the flow.
          args (list[str], optional):
              List of additional parameters configured for this flow.
              The user who submitted the load is always included.

      Returns:
          int_success (bool):
              Whether the load was successfully integrated (True or
              False).
          int_message (str):
              Exit message of the integration process.
          int_df (pyspark.sql.dataframe.DataFrame, optional):
              Spark dataframe with row-level error/non-error messages.
              The following structures are accepted:
                  - DataFrame[Row_ID: string, Message: string]
                  - DataFrame[Row_ID: string, Message: string, Is_Error: boolean]
              By default, all records are assumed to be error messages.
              The columns' order (index) is fixed; their name may be any.

      """
      # Ensure exclusive use for intended flow
      assert flow_key.upper() == FLOW_KEY.upper()
      
      # Integrate data into the curated table
      curated_integration_query={curated_query}

      logger.info("Curated integration query for LOAD=%s, FLOW=%s: %s",
                  load_key, flow_key, curated_integration_query)

      # Execute curated integration query
      spark.sql(curated_integration_query)

      # Integrate data into target table
      target_integration_query={dw_query}

      logger.info("Target integration query for LOAD=%s, FLOW=%s: %s",
                  load_key, flow_key, target_integration_query)

      # Execute integration query
      spark.sql(target_integration_query)

      # Signal integration success
      int_success = True
      int_message = "Successful integration"
      return int_success, int_message

      ''')
  
  def create_code_metadata(self):

    source_columns = ",\n            ".join(f"{cl.name} STRING" for cl in self.source_tables[0].columns)
    target_columns = ",\n              ".join(f"{cl.name} {cl.data_type} COMMENT '{cl.description}'" for cl in self.final_table.columns)

    return dedent(f'''\
      def create_metadata(load_key, flow_key, *args):
      """Ensure the metadata for this flow is created.

      Ensures the creation of the required RAW, CURATED and DW tables.

      Args:
          load_key (str):
              Identifier of the load.
          flow_key (str):
              Identifier of the flow.
          args (list[str], optional):
              List of additional parameters configured for this flow.
              The user who submitted the load is always included.

      """
      # Ensure exclusive use for intended flow
      assert flow_key.upper() == FLOW_KEY.upper()

      # Create source (raw) table
      create_source_table_query = f"""
          CREATE TABLE IF NOT EXISTS {{SOURCE_TABLE}} (
            -- System columns
            rowid                  STRING  COMMENT 'Identifier of the row in WDL',
            load_key               STRING  COMMENT 'Identifier of the load in WDL',
            -- Flow-specific columns
            {source_columns}
          )
          USING CSV
          LOCATION '{{SOURCE_LOCATION}}'
          OPTIONS (
          
            delimiter ";",
            header "true"
          )
          """

      logger.info(
          "Ensuring existence of source table '%s' for LOAD=%s, FLOW=%s: %s",
          SOURCE_TABLE, load_key, flow_key, create_source_table_query)

      spark.sql(create_source_table_query)
      
      # Create curated table
      create_curated_table_query = f"""
          CREATE TABLE IF NOT EXISTS {{CURATED_TABLE}} (
              {target_columns}
          )
          USING delta
          COMMENT ''
          TBLPROPERTIES (
            'Type' = 'MANAGED',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.minReaderVersion' = '1',
            'delta.minWriterVersion' = '2'
          )
          """

      logger.info(
          "Ensuring existence of curated table '%s' for LOAD=%s, FLOW=%s: %s",
          CURATED_TABLE, load_key, flow_key, create_curated_table_query)

      spark.sql(create_curated_table_query)

      # Create target (dw) table
      create_target_table_query = f"""
          CREATE TABLE IF NOT EXISTS {{TARGET_TABLE}} (
            {target_columns}
          )
          USING delta
          COMMENT '{self.final_table.description}'
  
          TBLPROPERTIES (
            'Type' = 'MANAGED',
            'delta.isolationLevel' = 'Serializable',
            'delta.minReaderVersion' = '1',
            'delta.minWriterVersion' = '2'
          )
        """

      logger.info(
          "Ensuring existence of target table '%s' for LOAD=%s, FLOW=%s: %s",
          TARGET_TABLE, load_key, flow_key, create_target_table_query)


      spark.sql(create_target_table_query)
      # Set target (dw) table tags
      set_target_table_tags_query = f"""
          ALTER TABLE {{TARGET_TABLE}}
          SET TAGS ('owner' = 'SSO', 'documentation' = '{self.confluence_url}')
          """

      logger.info(
          "Ensuring setting tags of target table '%s' for LOAD=%s, FLOW=%s: %s",
          TARGET_TABLE, load_key, flow_key, set_target_table_tags_query)

      spark.sql(set_target_table_tags_query)
      ''')

  def generate_notebook_code_string_format(self):
    return "".join([
      self.create_code_init(),
      self.create_code_validation(),
      self.create_code_integration(),
      self.create_code_metadata()
    ])
