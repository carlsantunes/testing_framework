from typing import List

from src.notebook.cells import NotebookCell
from src.notebook.models import NotebookSpec
from src.notebook.sections.section import Section
from src.notebook.cells.cell_helpers import md_cell, py_cell, sql_cell


class StartControlSection(Section):
    def enabled(self, spec: NotebookSpec) -> bool:
        return True

    def build(self, spec: NotebookSpec) -> List[NotebookCell]:
        return [
            md_cell("#### Initialize control table process"),
            py_cell(f"""
            # Parameter should be filled with the name of the DW table
            processName = {spec.notebook_name!r}

            # Register execution and obtain key assigned to process
            processKey = registerProcessExecution(processName)
            """),
        ]