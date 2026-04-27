from textwrap import dedent

from src.notebook.cells.notebook_cell import NotebookCell

def md_cell(text: str) -> NotebookCell:
    return NotebookCell(
        cell_type="markdown",
        content=dedent(text).strip()
    )

def py_cell(code: str) -> NotebookCell:
    return NotebookCell(
        cell_type="python",
        content=dedent(code).strip()
    )

def sql_cell(sql: str) -> NotebookCell:
    return NotebookCell(
        cell_type="sql",
        content=dedent(sql).strip()
    )

