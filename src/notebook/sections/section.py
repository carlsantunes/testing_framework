from abc import ABC, abstractmethod
from typing import List

from src.notebook.models import NotebookSpec
from src.notebook.cells import NotebookCell

class Section(ABC):
    @abstractmethod
    def enabled(self, spec: NotebookSpec) -> bool:
        ...

    @abstractmethod
    def build(self, spec: NotebookSpec) -> List[NotebookCell]:
        ...