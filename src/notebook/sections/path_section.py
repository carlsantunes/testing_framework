from typing import List

from src.notebook.cells import NotebookCell
from src.notebook.models import NotebookSpec
from src.notebook.sections.section import Section
from src.utils.cell_helpers import md_cell, py_cell, sql_cell

class PathsSection(Section):
    def enabled(self, spec: NotebookSpec) -> bool:
        return True

    def build(self, spec: NotebookSpec) -> List[NotebookCell]:
        return [
            md_cell("## Path Declaration")
        ]
