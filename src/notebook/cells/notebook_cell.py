from dataclasses import dataclass
from typing import Literal

CellType = Literal["python", "sql", "markdown"]

@dataclass
class NotebookCell:
    cell_type: CellType
    content: str