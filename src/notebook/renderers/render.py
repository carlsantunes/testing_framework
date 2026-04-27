from src.notebook.cells import NotebookCell

def _magic_cell(magic: str, content: str) -> str:
    lines = content.splitlines() or [""]
    return "\n".join([f"# MAGIC %{magic}"] + [f"# MAGIC {line}" for line in lines])

def render_cell(cell: NotebookCell) -> str:
    if cell.cell_type == "python":
        return cell.content
    if cell.cell_type == "sql":
        return _magic_cell("sql", cell.content)
    if cell.cell_type == "markdown":
        return _magic_cell("md", cell.content)
    raise ValueError(f"Unsupported cell type: {cell.cell_type}")

def render_notebook(cells: list[NotebookCell]) -> str:
    parts = ["# Databricks notebook source"]
    for i, cell in enumerate(cells):
        if i > 0:
            parts.append("# COMMAND ----------")
        parts.append(render_cell(cell))
    return "\n\n".join(parts) + "\n"