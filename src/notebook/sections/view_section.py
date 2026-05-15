from typing import List

from src.notebook.cells import NotebookCell
from src.notebook.models import NotebookSpec
from src.notebook.sections.section import Section
from src.notebook.cells.cell_helpers import md_cell, py_cell, sql_cell

class ViewCreationSection(Section):
    def enabled(self, spec: NotebookSpec) -> bool:
        return True

    def build(self, spec: NotebookSpec) -> List[NotebookCell]:
        colcast = ",\n    ".join(
            (
                "{"
                f"'col_order': {i + 1}, "
                f"'col_name': {col.name!r}, "
                f"'type': {col.data_type!r}, "
                f"'encrypted': {bool(col.flg_is_encrypted)!r}"
                "}"
            )
            for i, col in enumerate(spec.final_table.columns)
        )

        return [
            md_cell("## View Creation"),
            py_cell(f"""
            # Set parameters for target view creation function
            viewName = {f"{spec.final_table.catalog}.dw.vw_{spec.final_table.name}"!r}
            sourceTableNameOrStatement = {f"{spec.final_table.catalog}.{spec.final_table.schema}.{spec.final_table.name}"!r}

            colCastToDataType = [
                {colcast}
            ]
            viewComment = {getattr(spec.final_table, "view_description", "")!r}

            # Create target view
            udf_create_target_view_hr(
                viewName=viewName,
                sourceTableNameOrStatement=sourceTableNameOrStatement,
                colCastToDataType=colCastToDataType,
                viewComment=viewComment,
                recreateView=True
            )
            """),
        ]