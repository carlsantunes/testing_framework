from typing import List

from src.notebook.cells import NotebookCell
from src.notebook.models import NotebookSpec
from src.notebook.sections.section import Section
from src.notebook.cells.cell_helpers import md_cell, py_cell, sql_cell

class StagingSection(Section):
    def enabled(self, spec: NotebookSpec) -> bool:
        return True

    def build(self, spec: NotebookSpec) -> List[NotebookCell]:
        return [
            md_cell("### Overwrite Staging table"),
            py_cell(f"""
            df_final_encrypted.write.format("delta") \\
                .mode("overwrite") \\
                .option("overwriteSchema", "True") \\
                .saveAsTable("{spec.final_table.catalog}.stg.{spec.final_table.name}")
            """),
        ]
