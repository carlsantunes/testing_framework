from typing import List

from src.notebook.cells import NotebookCell
from src.notebook.models import NotebookSpec
from src.notebook.sections.section import Section
from src.notebook.cells.cell_helpers import md_cell, py_cell, sql_cell

from typing import Optional


def escape_sql_string(value: Optional[str]) -> str:
    if value is None:
        return ""
    return value.replace("'", "''")


class TableCreationSection(Section):
    def enabled(self, spec: NotebookSpec) -> bool:
        return bool(spec.final_table.columns)

    def build(self, spec: NotebookSpec) -> List[NotebookCell]:
        columns_statement = ",\n  ".join(
            f"{col.name} {getattr(col, 'data_type', 'STRING')} COMMENT '{escape_sql_string(col.description)}'"
            for col in spec.final_table.columns
        )

        table_comment = escape_sql_string(spec.final_table.description)

        return [
            md_cell("#### Table Creation"),
            sql_cell(f"""
            CREATE TABLE IF NOT EXISTS {spec.final_table.catalog}.{spec.final_table.schema}.{spec.final_table.name} (
              {columns_statement}
            )
            USING DELTA
            COMMENT '{table_comment}'
            """),
        ]

