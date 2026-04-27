from src.utils.logging_utils import log_setup_logic, log_info

from src.notebook.models.table_spec import TableSpec

## Notebook Class (stores notebook metadata)
class NotebookSpec:
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
  

class DWCLNotebook(NotebookSpec):
  def __init__(self, notebook_name, squad, source_tables, final_table, notebook_type, confluence_url, user_email):
    super().__init__(notebook_name, squad, source_tables, final_table, notebook_type, confluence_url, user_email)


class WDLNotebook(NotebookSpec):
  def __init__(self, notebook_name, sql_notebook_name, squad, source_tables, final_table, notebook_type, confluence_url, user_email, flow_owner, example_csv_row):
    super().__init__(notebook_name, squad, source_tables, final_table, notebook_type, confluence_url, user_email)
    self.sql_notebook_name = sql_notebook_name
    self.flow_owner = flow_owner
    self.example_csv_row = example_csv_row
    self.flow_key = self.final_table.name


class PreIngNotebook(NotebookSpec):
  def __init__(self, notebook_name, squad, source_tables, final_table, notebook_type, confluence_url, user_email):
    super().__init__(notebook_name, squad, source_tables, final_table, notebook_type, confluence_url, user_email)
    self.json_name = self.final_table.name.upper() + '.json'
