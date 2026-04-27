from typing import List

from src.notebook.cells import NotebookCell
from src.notebook.models import NotebookSpec
from src.notebook.sections.section import Section
from src.utils.cell_helpers import md_cell, py_cell, sql_cell

class EncryptionSection(Section):
    def enabled(self, spec: NotebookSpec) -> bool:
        return spec.final_table.flg_is_HR_table

    def build(self, spec: NotebookSpec) -> List[NotebookCell]:
        return [
            md_cell("#### Encrypt data"),
            py_cell("""
            list_type_1_cols = ['suk_date', 'buk_employee_office', 'buk_office']
            df_final_encrypted = get_encrypt_df_facs(df_final, type_1_cols=list_type_1_cols)
            """),
        ]

