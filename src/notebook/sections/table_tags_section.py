from typing import List

from src.notebook.cells import NotebookCell
from src.notebook.models import NotebookSpec
from src.notebook.sections.section import Section
from src.notebook.cells.cell_helpers import md_cell, py_cell, sql_cell

class TableTagsSection(Section):
    def enabled(self, spec: NotebookSpec) -> bool:
        return True

    def build(self, spec: NotebookSpec) -> List[NotebookCell]:
        fqtn = f"{spec.final_table.catalog}.{spec.final_table.schema}.{spec.final_table.name}"

        return [
            md_cell("##### Validation of Owner and Documentation Tags"),
            py_cell(f"""
            # Get table current tags
            df_tags = spark.sql(\"\"\"
            SELECT *
            FROM system.information_schema.table_tags
            WHERE catalog_name = '{spec.final_table.catalog}'
              AND schema_name = '{spec.final_table.schema}'
              AND table_name = '{spec.final_table.name}'
            \"\"\")

            # If owner and documentation tags are not present, then create them
            if df_tags.filter(df_tags["tag_name"] == "owner").limit(1).count() == 0:
                squad = {spec.squad!r}
                spark.sql(f\"\"\"ALTER TABLE {fqtn}
                SET TAGS ('owner' = '{{squad}}')\"\"\")

            if df_tags.filter(df_tags["tag_name"] == "documentation").limit(1).count() == 0:
                documentation = {spec.confluence_url!r}
                spark.sql(f\"\"\"ALTER TABLE {fqtn}
                SET TAGS ('documentation' = '{{documentation}}')\"\"\")
            """),
        ]