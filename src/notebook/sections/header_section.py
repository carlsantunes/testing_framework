from collections import defaultdict
from typing import Dict, List

from src.notebook.models import NotebookSpec, TableSpec
from src.notebook.sections.section import Section
from src.notebook.cells import NotebookCell
from src.utils.cell_helpers import md_cell


class HeaderSection(Section):
    """
    Builds the notebook header as a markdown cell.

    Responsibilities:
    - Group source tables by type
    - Render notebook objective metadata
    - Handle missing source table groups
    - Handle missing related view metadata
    """

    TABLE_TYPE_LABELS: Dict[str, str] = {
        "agg": "Aggregate Tables",
        "fac": "Factual Tables",
        "dim": "Dimension Tables",
        "cfg": "Configuration Tables",
        "cur": "Curated Tables",
    }

    def enabled(self, spec: NotebookSpec) -> bool:
        return spec.final_table is not None

    def build(self, spec: NotebookSpec) -> List[NotebookCell]:
        final_table = spec.final_table

        # Group source tables by type abbreviation
        grouped_tables = defaultdict(list)
        for table in spec.source_tables:
            grouped_tables[table.type_abv].append(
                f"{table.catalog}.{table.schema}.{table.name}"
            )

        # Build source table lines in a stable order
        source_table_lines = []
        for type_abv, label in self.TABLE_TYPE_LABELS.items():
            value = "; ".join(grouped_tables.get(type_abv, [])) or "N/A"
            source_table_lines.append(f"  * **{label}**: {value}")
        src_tables = "\n".join(source_table_lines)

        # Handle related view safely
        related_view = self._build_related_view(final_table)

        markdown = f"""
        ## Notebook Objectives:

        * **Goal:** Consolidation of {final_table.type} table
          `{final_table.catalog}.{final_table.schema}.{final_table.name}`

        * **SCD Type:** {final_table.scd_type or "N/A"}

        * **Source Tables**:

        {src_tables}

        - **Confluence link for table consolidation:** {spec.confluence_url or "N/A"}

        - **Related Views**: {related_view}

        - **Other Notes**: N/A
        """

        return [md_cell(markdown)]

    @staticmethod
    def _build_related_view(final_table: TableSpec) -> str:
        if all([
            final_table.view_catalog,
            final_table.view_schema,
            final_table.view_name
        ]):
            return (
                f"`{final_table.view_catalog}."
                f"{final_table.view_schema}."
                f"{final_table.view_name}`"
            )
        return "N/A"