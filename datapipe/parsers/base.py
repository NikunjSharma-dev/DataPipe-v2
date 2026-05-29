"""
datapipe/parsers/base.py — Abstract base parser.

Every parser implements __call__(path) → DataFrame | None.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

DEFAULT_CHUNK_SIZE    = 1_000
DEFAULT_CHUNK_OVERLAP = 100


class BaseParser(ABC):
    """All parsers must implement __call__ and extract."""

    file_type: str = "unknown"

    def __call__(self, path: Path) -> pd.DataFrame | None:
        """Convenience: call parser like a function."""
        return self.extract(path)

    @abstractmethod
    def extract(self, path: Path) -> pd.DataFrame | None:
        """Parse *path* and return a DataFrame, or None to skip."""
        ...

    # -- shared utilities ----------------------------------------------------

    @staticmethod
    def split_text(
        text: str,
        size: int = DEFAULT_CHUNK_SIZE,
        overlap: int = DEFAULT_CHUNK_OVERLAP,
    ) -> list[str]:
        """Sliding-window chunker. Returns list of non-empty strings."""
        text = text.strip()
        if not text:
            return []
        chunks: list[str] = []
        start = 0
        while start < len(text):
            chunk = text[start : start + size].strip()
            if chunk:
                chunks.append(chunk)
            start += size - overlap
        return chunks

    @staticmethod
    def clean(text: str) -> str:
        """Collapse whitespace and strip control characters."""
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", " ", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def sanitize_col(name: str) -> str:
        """Convert an arbitrary string to a safe SQLite column name."""
        name = re.sub(r"[^a-z0-9_]", "_", str(name).strip().lower())
        name = re.sub(r"_+", "_", name).strip("_")
        return name or "col"
