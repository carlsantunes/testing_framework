from typing import List

from src.notebook.cells import NotebookCell
from src.notebook.models import NotebookSpec
from src.notebook.sections.section import Section
from src.notebook.cells.cell_helpers import md_cell, py_cell, sql_cell


class DependenciesSection(Section):
    DEPENDENCY_TYPES = {"agg", "fac", "dim"}

    def enabled(self, spec: NotebookSpec) -> bool:
        return any(src.type_abv in self.DEPENDENCY_TYPES for src in spec.source_tables)

    def build(self, spec: NotebookSpec) -> List[NotebookCell]:
        dependency_tables = [
            src for src in spec.source_tables
            if src.type_abv in self.DEPENDENCY_TYPES
        ]

        lines = []
        df_names = []

        for src in dependency_tables:
            df_name = f"df_{src.name}_days_to_process"
            df_names.append(df_name)

            lines.extend([
                f"# Get days processed by trf_{src.name} since last execution",
                f"{df_name} = getDaysProcessedByDepedency(",
                f'    "trf_{src.name}", strLastRunStartDate',
                f")",
                "",
            ])

        # Safer and clearer than the original chained string-concatenation union
        if df_names:
            lines.append(f"df_records_to_process = {df_names[0]}")
            for df_name in df_names[1:]:
                lines.append(f"df_records_to_process = df_records_to_process.union({df_name})")
        else:
            lines.append("df_records_to_process = None")

        lines.extend([
            "",
            "df_records_to_process = df_records_to_process.dropDuplicates()",
            "",
            "# Check if there are any records to process",
            "if df_records_to_process.count() == 0:",
            "    exitNotebookOuput(",
            "        processKey,",
            "        processName,",
            '        "ALERT",',
            '        "No rows processed",',
            '        rowsRead="0",',
            '        rowsInserted="0",',
            '        rowsUpdated="0",',
            '        rowsDeleted="0",',
            "    )",
            "",
            "# Obtain the list of days and months to read from source tables",
            "lst_days_to_read, lst_months_to_read = getDaysMonthsToRead(df_records_to_process)",
        ])

        return [
            md_cell("##### Get Days To Process From Dependencies"),
            py_cell("\n".join(lines)),
        ]