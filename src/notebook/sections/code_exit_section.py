from typing import List

from src.notebook.cells import NotebookCell
from src.notebook.models import NotebookSpec
from src.notebook.sections.section import Section
from src.notebook.cells.cell_helpers import md_cell, py_cell, sql_cell

class ExitIfEmptySection(Section):
    def enabled(self, spec: NotebookSpec) -> bool:
        return True

    def build(self, spec: NotebookSpec) -> List[NotebookCell]:
        return [
            py_cell("""
            # Exit notebook with Warning if df_final is empty
            rows_read = df_final.count()

            if rows_read == 0:
                exitNotebookOuput(
                    processKey,
                    processName,
                    status="WARNING",
                    errorMessage="No rows read"
                )
            """)
        ]
