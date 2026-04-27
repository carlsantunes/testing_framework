from typing import List

from src.notebook.models import NotebookSpec
from src.notebook.renderers.render import render_notebook
from src.notebook.sections.section import Section


class NotebookGenerator:
    def __init__(self, sections: List[Section]):
        self.sections = sections

    def generate(self, spec: NotebookSpec) -> str:
        cells = []

        for section in self.sections:
            if section.enabled(spec):
                cells.extend(section.build(spec))

        return render_notebook(cells)
