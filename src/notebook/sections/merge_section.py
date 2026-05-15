from typing import List

from src.notebook.cells import NotebookCell
from src.notebook.models import NotebookSpec
from src.notebook.sections.section import Section
from src.notebook.cells.cell_helpers import md_cell, py_cell, sql_cell

class MergeDWSection(Section):
    def enabled(self, spec: NotebookSpec) -> bool:
        return True

    def build(self, spec: NotebookSpec) -> List[NotebookCell]:
        primary_keys = getattr(spec.final_table, "primary_keys", None) or getattr(spec.final_table, "primary_key", [])
        columns_to_update = spec.final_table.columns_to_update
        columns = spec.final_table.columns

        on_clause = " AND\n  ".join(
            f"target.{key} = source.{key}"
            for key in primary_keys
        )

        update_clause = ",\n  ".join(
            "target.dtm_updated_at = current_timestamp()"
            if column_name == "dtm_updated_at"
            else f"target.{column_name} = source.{column_name}"
            for column_name in columns_to_update
        )

        insert_clause = ",\n  ".join(
            col.name for col in columns
        )

        values_clause = ",\n  ".join(
            "current_timestamp()"
            if col.name in {"dtm_created_at", "dtm_updated_at"}
            else f"source.{col.name}"
            for col in columns
        )

        return [
            md_cell("### Write into Data Lake"),
            sql_cell(f"""
            MERGE INTO {spec.final_table.catalog}.{spec.final_table.schema}.{spec.final_table.name} AS target
            USING {spec.final_table.catalog}.stg.{spec.final_table.name} AS source
            ON {on_clause}
            WHEN MATCHED THEN
              UPDATE
              SET
                {update_clause}
            WHEN NOT MATCHED THEN
              INSERT
              (
                {insert_clause}
              )
              VALUES
              (
                {values_clause}
              )
            """),
        ]