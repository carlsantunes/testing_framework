from src.notebook.cells.cell_helpers import md_cell, run_cell
from src.notebook.config import NotebookPaths
from src.notebook.models import NotebookSpec
from src.notebook.cells import NotebookCell
from src.notebook.sections.section import Section

from typing import List

class UtilitiesSection(Section):
    def __init__(self, paths: NotebookPaths):
        self.paths = paths

    def enabled(self, spec: NotebookSpec) -> bool:
        return True

    def build(self, spec: NotebookSpec) -> List[NotebookCell]:
        cells = [
            md_cell("## Invoke Utility Notebook"),
            run_cell(self.paths.control_utils),
        ]

        if spec.final_table.flg_is_HR_table:
            cells.extend(
                [
                    run_cell(self.paths.hr_decrypted_view_creation),
                    run_cell(self.paths.hr_utils),
                ]
            )

        return cells
